package raft

import (
	"context"
	"log/slog"

	pb "granite/proto"
)

func (n *Node) sendHeartbeats(term uint64) {
	n.replicateToPeers(term)
}

func (n *Node) replicateToPeers(term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	peers := n.peerPool.PeerIDs()
	n.mu.Unlock()

	for _, peerID := range peers {
		go n.replicateToPeer(peerID, term)
	}
}

func (n *Node) replicateToPeer(peerID string, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = 1
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64 = 0

	if prevLogIndex < uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	var entries []*pb.LogEntry
	if nextIdx < uint64(len(n.log)) {
		entries = make([]*pb.LogEntry, len(n.log)-int(nextIdx))
		copy(entries, n.log[nextIdx:])
	}

	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval)
	defer cancel()

	resp, err := n.peerPool.AppendEntries(ctx, peerID, req)
	if err != nil {
		slog.Debug("raft: AppendEntries RPC failed", "leader", n.id, "peer", peerID, "error", err)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.role = Follower
		n.votedFor = ""
		n.persistStateLocked()
		n.resetElectionTimerLocked()
		return
	}

	if n.role != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {

		if len(entries) > 0 {
			lastNewEntryIndex := entries[len(entries)-1].Index
			n.matchIndex[peerID] = lastNewEntryIndex
			n.nextIndex[peerID] = lastNewEntryIndex + 1

			slog.Debug("raft: follower updated matchIndex",
				"peer", peerID,
				"matchIndex", lastNewEntryIndex,
			)

			n.maybeAdvanceCommitIndexLocked()
		}
	} else {

		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
			slog.Debug("raft: log inconsistency, backing up nextIndex",
				"peer", peerID,
				"newNextIndex", n.nextIndex[peerID],
			)
		}
	}
}

func (n *Node) HandleAppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:       n.currentTerm,
		Success:    false,
		MatchIndex: 0,
	}

	if req.Term < n.currentTerm {
		return resp, nil
	}

	if req.Term > n.currentTerm || n.role == Candidate {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
		n.persistStateLocked()
	}

	n.resetElectionTimerLocked()

	if req.PrevLogIndex >= uint64(len(n.log)) {
		return resp, nil
	}

	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {

		n.log = n.log[:req.PrevLogIndex]
		n.persistStateLocked()
		return resp, nil
	}

	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {

				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}

		} else {

			n.log = append(n.log, entry)
		}
	}

	n.persistStateLocked()

	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := n.lastLogIndexLocked()
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}

		n.applyCommittedEntriesLocked()
	}

	resp.Success = true
	resp.MatchIndex = n.lastLogIndexLocked()
	return resp, nil
}

func (n *Node) maybeAdvanceCommitIndexLocked() {
	quorum := n.cfg.Quorum()

	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {

		if n.log[N].Term != n.currentTerm {
			continue
		}

		count := 1
		for _, peerID := range n.peerPool.PeerIDs() {
			if n.matchIndex[peerID] >= N {
				count++
			}
		}

		if count >= quorum {
			slog.Info("raft: ADVANCING COMMIT INDEX",
				"node", n.id,
				"oldCommitIndex", n.commitIndex,
				"newCommitIndex", N,
				"quorum", count,
			)
			n.commitIndex = N
			n.applyCommittedEntriesLocked()
			break
		}
	}
}

func (n *Node) applyCommittedEntriesLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Data,
			CommandIndex: entry.Index,
		}

		slog.Debug("raft: applying entry to state machine",
			"node", n.id,
			"index", entry.Index,
		)

		select {
		case n.applyCh <- msg:
		default:
			go func(m ApplyMsg) {
				n.applyCh <- m
			}(msg)
		}
	}
}
