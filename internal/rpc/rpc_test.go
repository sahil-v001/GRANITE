package rpc

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"granite/internal/storage"
	pb "granite/proto"
)

func TestNodeToNodePing(t *testing.T) {
	tmpDir := t.TempDir()

	nodeBData := filepath.Join(tmpDir, "nodeB")
	engineB, err := storage.Open(nodeBData)
	if err != nil {
		t.Fatalf("Failed to open engine B: %v", err)
	}
	defer engineB.Close()

	serverB := NewGRPCServer("nodeB", nil, engineB)
	go func() {
		if err := serverB.Start("127.0.0.1:5002"); err != nil {
			t.Logf("Server B stopped: %v", err)
		}
	}()
	defer serverB.Stop()

	nodeAData := filepath.Join(tmpDir, "nodeA")
	engineA, err := storage.Open(nodeAData)
	if err != nil {
		t.Fatalf("Failed to open engine A: %v", err)
	}
	defer engineA.Close()

	serverA := NewGRPCServer("nodeA", nil, engineA)
	go func() {
		if err := serverA.Start("127.0.0.1:5001"); err != nil {
			t.Logf("Server A stopped: %v", err)
		}
	}()
	defer serverA.Stop()

	time.Sleep(100 * time.Millisecond)

	poolA := NewPeerPool("nodeA")
	defer poolA.Close()

	if err := poolA.Connect("nodeB", "127.0.0.1:5002"); err != nil {
		t.Fatalf("Node A failed to connect to Node B: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pingReq := &pb.PingRequest{SenderId: "nodeA"}
	resp, err := poolA.Ping(ctx, "nodeB", pingReq)
	if err != nil {
		t.Fatalf("Node A Ping to Node B failed: %v", err)
	}

	if resp.ResponderId != "nodeB" {
		t.Errorf("Expected ResponderId 'nodeB', got %q", resp.ResponderId)
	}
	expectedMsg := `node "nodeB" is alive and ready`
	if resp.Message != expectedMsg {
		t.Errorf("Expected Message %q, got %q", expectedMsg, resp.Message)
	}

	t.Logf("✅ SUCCESS: Node A successfully pinged Node B over gRPC TCP! Response: %s", resp.Message)
}

func TestBroadcastPing(t *testing.T) {
	tmpDir := t.TempDir()

	peers := map[string]string{
		"node2": "127.0.0.1:5003",
		"node3": "127.0.0.1:5004",
	}

	for id, addr := range peers {
		dir := filepath.Join(tmpDir, id)
		eng, err := storage.Open(dir)
		if err != nil {
			t.Fatalf("Failed to open engine %s: %v", id, err)
		}
		defer eng.Close()

		srv := NewGRPCServer(id, nil, eng)
		go func(s *GRPCServer, a string) {
			_ = s.Start(a)
		}(srv, addr)
		defer srv.Stop()
	}

	time.Sleep(100 * time.Millisecond)

	pool1 := NewPeerPool("node1")
	defer pool1.Close()

	for id, addr := range peers {
		if err := pool1.Connect(id, addr); err != nil {
			t.Fatalf("Failed to connect to %s: %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results := pool1.BroadcastPing(ctx, &pb.PingRequest{SenderId: "node1"})

	if len(results) != 2 {
		t.Fatalf("Expected 2 ping results, got %d", len(results))
	}

	for id, res := range results {
		if res.Err != nil {
			t.Errorf("Ping to %s failed: %v", id, res.Err)
		} else if res.Response.ResponderId != id {
			t.Errorf("Expected ResponderId %s, got %s", id, res.Response.ResponderId)
		}
	}

	t.Logf("✅ SUCCESS: BroadcastPing concurrently reached all nodes in cluster pool!")
}
