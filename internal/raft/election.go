package raft

import (
	"context"
	"log/slog"
	"time"

	pb "granite/proto"
)

func (n *Node) onElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {
		return
	}

	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.persistStateLocked()
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()

	slog.Info("raft: starting election",
		"node", n.id,
		"term", term,
		"lastLogIndex", lastIndex,
		"lastLogTerm", lastTerm,
	)

	go n.startElection(term, lastIndex, lastTerm)
}

func (n *Node) startElection(term uint64, lastLogIndex uint64, lastLogTerm uint64) {
	peers := n.peerPool.PeerIDs()
	votesReceived := 1

	quorum := n.cfg.Quorum()

	voteCh := make(chan bool, len(peers))

	req := &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peerID := range peers {
		go func(pID string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.HeartbeatInterval*2)
			defer cancel()

			resp, err := n.peerPool.RequestVote(ctx, pID, req)
			if err != nil {
				slog.Debug("raft: vote request failed", "candidate", n.id, "peer", pID, "error", err)
				voteCh <- false
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
				voteCh <- false
				return
			}

			if n.role == Candidate && n.currentTerm == term && resp.VoteGranted {
				voteCh <- true
			} else {
				voteCh <- false
			}
		}(peerID)
	}

	for range peers {
		if <-voteCh {
			votesReceived++
			if votesReceived >= quorum {
				n.mu.Lock()
				if n.role == Candidate && n.currentTerm == term {
					slog.Info("raft: ELECTED LEADER",
						"node", n.id,
						"term", term,
						"votes", votesReceived,
						"quorum", quorum,
					)

					n.role = Leader
					n.lastHeartbeat = timeNow()

					for _, pID := range n.peerPool.PeerIDs() {
						n.nextIndex[pID] = n.lastLogIndexLocked() + 1
						n.matchIndex[pID] = 0
					}

					go n.sendHeartbeats(term)
				}
				n.mu.Unlock()
				return
			}
		}
	}
}

func (n *Node) HandleRequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if req.Term < n.currentTerm {
		return resp, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
		n.persistStateLocked()
	}

	canVote := n.votedFor == "" || n.votedFor == req.CandidateId

	lastTerm := n.lastLogTermLocked()
	lastIndex := n.lastLogIndexLocked()

	logUpToDate := false
	if req.LastLogTerm > lastTerm {
		logUpToDate = true
	} else if req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex {
		logUpToDate = true
	}

	if canVote && logUpToDate {
		n.votedFor = req.CandidateId
		n.persistStateLocked()
		n.resetElectionTimerLocked()

		resp.VoteGranted = true
		slog.Info("raft: granted vote",
			"node", n.id,
			"candidate", req.CandidateId,
			"term", req.Term,
		)
	}

	resp.Term = n.currentTerm
	return resp, nil
}

var timeNow = func() time.Time {
	return time.Now()
}
