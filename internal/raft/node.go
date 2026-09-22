package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"granite/internal/config"
	"granite/internal/rpc"
	pb "granite/proto"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

type Node struct {
	mu sync.Mutex

	id          string
	currentTerm uint64
	votedFor    string
	log         []*pb.LogEntry

	commitIndex uint64
	lastApplied uint64
	role        Role

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	lastHeartbeat  time.Time

	cfg      *config.Config
	peerPool *rpc.PeerPool
	applyCh  chan ApplyMsg

	dataDir string
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewNode(cfg *config.Config, pool *rpc.PeerPool, applyCh chan ApplyMsg) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("raft: invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		id:          cfg.NodeID,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		role:        Follower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		cfg:         cfg,
		peerPool:    pool,
		applyCh:     applyCh,
		dataDir:     cfg.DataDir,
		ctx:         ctx,
		cancel:      cancel,
	}

	n.log = append(n.log, &pb.LogEntry{Index: 0, Term: 0, Data: nil})

	if err := n.loadState(); err != nil {
		slog.Warn("raft: could not load state from disk, starting fresh", "node", n.id, "error", err)
	}

	return n, nil
}

func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()

	slog.Info("raft: starting node", "node", n.id, "role", n.role, "term", n.currentTerm)

	n.resetElectionTimerLocked()

	go n.runEventLoop()
}

func (n *Node) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()

	slog.Info("raft: stopping node", "node", n.id)
	n.cancel()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
}

func (n *Node) Propose(data []byte) (uint64, uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, false
	}

	newIndex := n.lastLogIndexLocked() + 1
	newTerm := n.currentTerm

	entry := &pb.LogEntry{
		Index: newIndex,
		Term:  newTerm,
		Data:  data,
	}

	n.log = append(n.log, entry)
	n.persistStateLocked()

	slog.Info("raft: leader proposed new entry",
		"node", n.id,
		"index", newIndex,
		"term", newTerm,
	)

	go n.replicateToPeers(newTerm)

	return newIndex, newTerm, true
}

func (n *Node) GetState() (uint64, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

func (n *Node) GetRole() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) lastLogIndexLocked() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	return n.log[len(n.log)-1].Term
}

func (n *Node) runEventLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
			time.Sleep(10 * time.Millisecond)
			n.checkTimers()
		}
	}
}

func (n *Node) checkTimers() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {

		if time.Since(n.lastHeartbeat) >= n.cfg.HeartbeatInterval {
			n.lastHeartbeat = time.Now()
			term := n.currentTerm
			go n.sendHeartbeats(term)
		}
	}
}

func (n *Node) resetElectionTimerLocked() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	minMs := int(n.cfg.ElectionTimeoutMin.Milliseconds())
	maxMs := int(n.cfg.ElectionTimeoutMax.Milliseconds())
	timeoutMs := minMs + rand.Intn(maxMs-minMs+1)
	timeout := time.Duration(timeoutMs) * time.Millisecond

	n.electionTimer = time.AfterFunc(timeout, func() {
		n.onElectionTimeout()
	})
}

func (n *Node) stateFilePath() string {
	return filepath.Join(n.dataDir, "raft_state.bin")
}

func (n *Node) persistStateLocked() {
	_ = os.MkdirAll(n.dataDir, 0755)
	file, err := os.Create(n.stateFilePath() + ".tmp")
	if err != nil {
		slog.Error("raft: failed to create state file", "node", n.id, "error", err)
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, n.currentTerm)
	_, _ = file.Write(buf)

	votedBytes := []byte(n.votedFor)
	binary.LittleEndian.PutUint16(buf[:2], uint16(len(votedBytes)))
	_, _ = file.Write(buf[:2])
	if len(votedBytes) > 0 {
		_, _ = file.Write(votedBytes)
	}

	binary.LittleEndian.PutUint32(buf[:4], uint32(len(n.log)))
	_, _ = file.Write(buf[:4])

	for _, entry := range n.log {
		binary.LittleEndian.PutUint64(buf, entry.Index)
		_, _ = file.Write(buf)
		binary.LittleEndian.PutUint64(buf, entry.Term)
		_, _ = file.Write(buf)
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(entry.Data)))
		_, _ = file.Write(buf[:4])
		if len(entry.Data) > 0 {
			_, _ = file.Write(entry.Data)
		}
	}

	_ = file.Sync()
	_ = file.Close()
	_ = os.Rename(n.stateFilePath()+".tmp", n.stateFilePath())
}

func (n *Node) loadState() error {
	path := n.stateFilePath()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	var buf [8]byte
	if _, err := io.ReadFull(file, buf[:8]); err != nil {
		return err
	}
	n.currentTerm = binary.LittleEndian.Uint64(buf[:8])

	if _, err := io.ReadFull(file, buf[:2]); err != nil {
		return err
	}
	votedLen := binary.LittleEndian.Uint16(buf[:2])
	if votedLen > 0 {
		votedBytes := make([]byte, votedLen)
		if _, err := io.ReadFull(file, votedBytes); err != nil {
			return err
		}
		n.votedFor = string(votedBytes)
	}

	if _, err := io.ReadFull(file, buf[:4]); err != nil {
		return err
	}
	logCount := binary.LittleEndian.Uint32(buf[:4])

	if logCount > 0 {
		n.log = make([]*pb.LogEntry, 0, logCount)
		for i := uint32(0); i < logCount; i++ {
			if _, err := io.ReadFull(file, buf[:8]); err != nil {
				return err
			}
			idx := binary.LittleEndian.Uint64(buf[:8])

			if _, err := io.ReadFull(file, buf[:8]); err != nil {
				return err
			}
			term := binary.LittleEndian.Uint64(buf[:8])

			if _, err := io.ReadFull(file, buf[:4]); err != nil {
				return err
			}
			dataLen := binary.LittleEndian.Uint32(buf[:4])

			var data []byte
			if dataLen > 0 {
				data = make([]byte, dataLen)
				if _, err := io.ReadFull(file, data); err != nil {
					return err
				}
			}

			n.log = append(n.log, &pb.LogEntry{
				Index: idx,
				Term:  term,
				Data:  data,
			})
		}
	}

	slog.Info("raft: restored persistent state from disk",
		"node", n.id,
		"term", n.currentTerm,
		"votedFor", n.votedFor,
		"logEntries", len(n.log),
	)

	return nil
}
