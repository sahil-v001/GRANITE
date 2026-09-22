package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	pb "granite/proto"
)

func (n *Node) TakeSnapshot(lastIncludedIndex uint64, snapshotData []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if lastIncludedIndex <= n.log[0].Index || lastIncludedIndex > n.lastApplied {
		return fmt.Errorf("raft: invalid snapshot index %d (lastApplied=%d, snapshotPoint=%d)",
			lastIncludedIndex, n.lastApplied, n.log[0].Index)
	}

	var cutIdx int = -1
	for i, entry := range n.log {
		if entry.Index == lastIncludedIndex {
			cutIdx = i
			break
		}
	}

	if cutIdx == -1 {
		return fmt.Errorf("raft: snapshot index %d not found in log", lastIncludedIndex)
	}

	lastIncludedTerm := n.log[cutIdx].Term

	newLog := make([]*pb.LogEntry, 0, len(n.log)-cutIdx)
	newLog = append(newLog, &pb.LogEntry{
		Index: lastIncludedIndex,
		Term:  lastIncludedTerm,
		Data:  nil,
	})
	newLog = append(newLog, n.log[cutIdx+1:]...)

	n.log = newLog
	n.persistStateLocked()

	if err := n.saveSnapshotFileLocked(lastIncludedIndex, lastIncludedTerm, snapshotData); err != nil {
		return fmt.Errorf("raft: failed to save snapshot to disk: %w", err)
	}

	slog.Info("raft: snapshot taken and log truncated",
		"node", n.id,
		"lastIncludedIndex", lastIncludedIndex,
		"lastIncludedTerm", lastIncludedTerm,
		"remainingLogEntries", len(n.log),
	)

	return nil
}

func (n *Node) HandleInstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.InstallSnapshotResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp, nil
	}

	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
		n.persistStateLocked()
	}

	n.resetElectionTimerLocked()

	if req.LastIncludedIndex <= n.commitIndex {
		return resp, nil
	}

	newLog := make([]*pb.LogEntry, 0)
	newLog = append(newLog, &pb.LogEntry{
		Index: req.LastIncludedIndex,
		Term:  req.LastIncludedTerm,
		Data:  nil,
	})

	n.log = newLog
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex
	n.persistStateLocked()

	_ = n.saveSnapshotFileLocked(req.LastIncludedIndex, req.LastIncludedTerm, req.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}

	select {
	case n.applyCh <- msg:
	default:
		go func(m ApplyMsg) {
			n.applyCh <- m
		}(msg)
	}

	slog.Info("raft: snapshot installed from leader",
		"node", n.id,
		"lastIncludedIndex", req.LastIncludedIndex,
		"lastIncludedTerm", req.LastIncludedTerm,
	)

	return resp, nil
}

func (n *Node) snapshotFilePath() string {
	return filepath.Join(n.dataDir, "snapshot.bin")
}

func (n *Node) saveSnapshotFileLocked(index uint64, term uint64, data []byte) error {
	_ = os.MkdirAll(n.dataDir, 0755)
	tmpPath := n.snapshotFilePath() + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	_, _ = file.Write(data)
	_ = file.Sync()
	_ = file.Close()
	return os.Rename(tmpPath, n.snapshotFilePath())
}

func (n *Node) LoadSnapshotFile() ([]byte, error) {
	file, err := os.Open(n.snapshotFilePath())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}
