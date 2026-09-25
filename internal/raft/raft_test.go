package raft

import (
	"path/filepath"
	"testing"
	"time"

	"granite/internal/config"
	"granite/internal/rpc"
	"granite/internal/storage"
)

type testNode struct {
	id        string
	addr      string
	dataDir   string
	cfg       *config.Config
	engine    *storage.Engine
	applyCh   chan ApplyMsg
	stateMach *StateMachine
	raftNode  *Node
	rpcServer *rpc.GRPCServer
	peerPool  *rpc.PeerPool
}

func setupTestCluster(t *testing.T, tmpDir string) ([]*testNode, func()) {
	t.Helper()

	nodeIDs := []string{"node1", "node2", "node3"}
	addrs := map[string]string{
		"node1": "127.0.0.1:6001",
		"node2": "127.0.0.1:6002",
		"node3": "127.0.0.1:6003",
	}

	nodes := make([]*testNode, 3)

	for i, id := range nodeIDs {
		dataDir := filepath.Join(tmpDir, id)
		eng, err := storage.Open(dataDir)
		if err != nil {
			t.Fatalf("Failed to open storage engine for %s: %v", id, err)
		}

		peers := make([]string, 0)
		for peerID, peerAddr := range addrs {
			if peerID != id {
				peers = append(peers, peerAddr)
			}
		}

		cfg := &config.Config{
			NodeID:             id,
			ListenAddr:         addrs[id],
			Peers:              peers,
			DataDir:            dataDir,
			MemTableMaxBytes:   64 * 1024 * 1024,
			HeartbeatInterval:  40 * time.Millisecond,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
		}

		applyCh := make(chan ApplyMsg, 100)
		sm := NewStateMachine(eng, applyCh)
		sm.Start()

		pool := rpc.NewPeerPool(id)

		rNode, err := NewNode(cfg, pool, applyCh)
		if err != nil {
			t.Fatalf("Failed to create Raft node %s: %v", id, err)
		}

		srv := rpc.NewGRPCServer(id, rNode, eng)

		tn := &testNode{
			id:        id,
			addr:      addrs[id],
			dataDir:   dataDir,
			cfg:       cfg,
			engine:    eng,
			applyCh:   applyCh,
			stateMach: sm,
			raftNode:  rNode,
			rpcServer: srv,
			peerPool:  pool,
		}

		nodes[i] = tn
	}

	for _, tn := range nodes {
		addr := tn.addr
		srv := tn.rpcServer
		go func() {
			_ = srv.Start(addr)
		}()
	}

	time.Sleep(100 * time.Millisecond)

	for _, tn := range nodes {
		for peerID, peerAddr := range addrs {
			if peerID != tn.id {
				_ = tn.peerPool.Connect(peerID, peerAddr)
			}
		}
	}

	for _, tn := range nodes {
		tn.raftNode.Start()
	}

	cleanup := func() {
		for _, tn := range nodes {
			tn.raftNode.Stop()
			tn.rpcServer.Stop()
			tn.peerPool.Close()
			tn.stateMach.Stop()
			tn.engine.Close()
		}
	}

	return nodes, cleanup
}

func findLeader(nodes []*testNode) *testNode {
	for _, tn := range nodes {
		if _, isLeader := tn.raftNode.GetState(); isLeader {
			return tn
		}
	}
	return nil
}

func TestRaftLeaderElection(t *testing.T) {
	tmpDir := t.TempDir()
	nodes, cleanup := setupTestCluster(t, tmpDir)
	defer cleanup()

	var leader *testNode
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		leader = findLeader(nodes)
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leader == nil {
		t.Fatalf("FAILED: No leader was elected after 2 seconds")
	}

	term, _ := leader.raftNode.GetState()
	t.Logf("✅ SUCCESS: Leader elected: %s (Term %d)", leader.id, term)

	leaderCount := 0
	for _, tn := range nodes {
		if _, isLeader := tn.raftNode.GetState(); isLeader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("Expected exactly 1 leader, got %d", leaderCount)
	}
}

func TestRaftLogReplication(t *testing.T) {
	tmpDir := t.TempDir()
	nodes, cleanup := setupTestCluster(t, tmpDir)
	defer cleanup()

	var leader *testNode
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		leader = findLeader(nodes)
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leader == nil {
		t.Fatalf("No leader elected")
	}

	cmd := EncodeCommand(storage.OpPut, []byte("leader_key"), []byte("leader_value"))
	idx, term, isLeader := leader.raftNode.Propose(cmd)
	if !isLeader {
		t.Fatalf("Node %s was expected to be leader", leader.id)
	}

	t.Logf("Proposed command to leader %s at Index %d, Term %d", leader.id, idx, term)

	time.Sleep(500 * time.Millisecond)

	for _, tn := range nodes {
		val, err := tn.engine.Get([]byte("leader_key"))
		if err != nil {
			t.Errorf("Node %s failed to get replicated key: %v", tn.id, err)
		} else if string(val) != "leader_value" {
			t.Errorf("Node %s expected value 'leader_value', got %q", tn.id, string(val))
		} else {
			t.Logf("  ✓ Node %s correctly replicated and applied leader_key -> %s", tn.id, string(val))
		}
	}

	t.Logf("✅ SUCCESS: Log replication verified across all cluster nodes!")
}

func TestRaftLeaderFailover(t *testing.T) {
	tmpDir := t.TempDir()
	nodes, cleanup := setupTestCluster(t, tmpDir)
	defer cleanup()

	var leader1 *testNode
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		leader1 = findLeader(nodes)
		if leader1 != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leader1 == nil {
		t.Fatalf("No leader elected")
	}

	t.Logf("Initial Leader is: %s", leader1.id)

	t.Logf("💥 Killing Leader %s...", leader1.id)
	leader1.raftNode.Stop()
	leader1.rpcServer.Stop()

	var leader2 *testNode
	for start := time.Now(); time.Since(start) < 2*time.Second; {
		for _, tn := range nodes {
			if tn.id != leader1.id {
				if _, isLeader := tn.raftNode.GetState(); isLeader {
					leader2 = tn
					break
				}
			}
		}
		if leader2 != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leader2 == nil {
		t.Fatalf("FAILED: Remaining 2 nodes failed to elect a new leader after crash!")
	}

	term2, _ := leader2.raftNode.GetState()
	t.Logf("✅ SUCCESS: Cluster healed! New Leader elected: %s (Term %d)", leader2.id, term2)

	cmd := EncodeCommand(storage.OpPut, []byte("post_failover_key"), []byte("healed_cluster"))
	idx, _, isLeader := leader2.raftNode.Propose(cmd)
	if !isLeader || idx == 0 {
		t.Fatalf("New leader failed to accept write after failover")
	}

	time.Sleep(300 * time.Millisecond)

	val, err := leader2.engine.Get([]byte("post_failover_key"))
	if err != nil || string(val) != "healed_cluster" {
		t.Fatalf("New leader failed to apply post-failover write: %v", err)
	}

	t.Logf("  ✓ New leader %s successfully accepted and applied write 'post_failover_key' -> 'healed_cluster'", leader2.id)
}
