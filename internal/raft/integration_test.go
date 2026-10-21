package raft

import (
	"context"
	"testing"
	"time"

	"granite/internal/rpc"
	pb "granite/proto"
)

func TestPhase4ClusterIntegration(t *testing.T) {
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
		t.Fatalf("FAILED: No leader elected after 2 seconds")
	}

	t.Logf("✅ Cluster elected Leader: %s", leader.id)

	var follower *testNode
	for _, tn := range nodes {
		if tn.id != leader.id {
			follower = tn
			break
		}
	}

	clientPool := rpc.NewPeerPool("client")
	defer clientPool.Close()
	_ = clientPool.Connect(follower.id, follower.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	folResp, err := clientPool.Put(ctx, follower.id, &pb.PutRequest{
		Key:   []byte("follower_key"),
		Value: []byte("follower_value"),
	})
	if err != nil {
		t.Fatalf("Client gRPC call to follower failed: %v", err)
	}
	if folResp.Success || folResp.Error != "not leader" {
		t.Fatalf("Expected follower write to be rejected with 'not leader', got success=%v, err=%s", folResp.Success, folResp.Error)
	}

	t.Logf("  ✓ Follower %s correctly rejected write with '%s'", follower.id, folResp.Error)

	_ = clientPool.Connect(leader.id, leader.addr)
	leadResp, err := clientPool.Put(ctx, leader.id, &pb.PutRequest{
		Key:   []byte("cluster_key_1"),
		Value: []byte("cluster_value_1"),
	})
	if err != nil || !leadResp.Success {
		t.Fatalf("Client write to leader %s failed: %v, resp=%v", leader.id, err, leadResp)
	}

	t.Logf("  ✓ Leader %s accepted client write 'cluster_key_1' -> 'cluster_value_1'", leader.id)

	time.Sleep(400 * time.Millisecond)

	for _, tn := range nodes {
		val, err := tn.engine.Get([]byte("cluster_key_1"))
		if err != nil || string(val) != "cluster_value_1" {
			t.Errorf("Node %s missing or invalid value: %v, val=%s", tn.id, err, string(val))
		} else {
			t.Logf("  ✓ LSM verify: Node %s has 'cluster_key_1' = %q", tn.id, string(val))
		}
	}

	t.Logf("✅ SUCCESS: Phase 4 Full Integration (Client -> Raft -> LSM) Verified!")
}
