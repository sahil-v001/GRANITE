package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"granite/internal/rpc"
	pb "granite/proto"
)

func TestChaosClusterFaultTolerance(t *testing.T) {
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
		t.Fatalf("FAILED: No initial leader elected")
	}

	t.Logf("⚡ Chaos Test Started. Initial Leader: %s", leader.id)

	clientPool := rpc.NewPeerPool("chaos_client")
	defer clientPool.Close()

	for _, tn := range nodes {
		_ = clientPool.Connect(tn.id, tn.addr)
	}

	var mu sync.Mutex
	committedKeys := make(map[string]string)
	stopCh := make(chan struct{})

	go func() {
		keyIdx := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				currentLeader := findLeader(nodes)
				if currentLeader != nil {
					k := fmt.Sprintf("chaos_key_%04d", keyIdx)
					v := fmt.Sprintf("chaos_val_%04d", keyIdx)

					ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
					resp, err := clientPool.Put(ctx, currentLeader.id, &pb.PutRequest{
						Key:   []byte(k),
						Value: []byte(v),
					})
					cancel()

					if err == nil && resp.Success {
						mu.Lock()
						committedKeys[k] = v
						mu.Unlock()
						keyIdx++
					}
				}
				time.Sleep(30 * time.Millisecond)
			}
		}
	}()

	for i := 0; i < 2; i++ {
		time.Sleep(400 * time.Millisecond)

		targetIdx := rand.Intn(len(nodes))
		target := nodes[targetIdx]

		t.Logf("🔥 Chaos Event #%d: Temporarily interrupting %s...", i+1, target.id)
		target.raftNode.Stop()
		time.Sleep(300 * time.Millisecond)
		target.raftNode.Start()
		t.Logf("  ✓ Node %s revived!", target.id)
	}

	close(stopCh)

	time.Sleep(200 * time.Millisecond)
	finalLeader := findLeader(nodes)
	if finalLeader != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, _ = clientPool.Put(ctx, finalLeader.id, &pb.PutRequest{
			Key:   []byte("chaos_sync_key"),
			Value: []byte("sync_val"),
		})
		cancel()
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	totalKeys := len(committedKeys)
	keysCopy := make(map[string]string, totalKeys)
	for k, v := range committedKeys {
		keysCopy[k] = v
	}
	mu.Unlock()

	t.Logf("📊 Chaos writing finished. Total committed keys: %d", totalKeys)

	finalLeader = findLeader(nodes)
	if finalLeader == nil {
		t.Fatalf("FAILED: No leader active after chaos run")
	}

	missing := 0
	for k, expectedV := range keysCopy {
		val, err := finalLeader.engine.Get([]byte(k))
		if err != nil || string(val) != expectedV {
			missing++
			t.Errorf("Missing key after chaos: key=%s, err=%v, got=%s", k, err, string(val))
		}
	}

	if missing == 0 {
		t.Logf("✅ PASS — Zero data loss across all %d committed keys after Chaos disruption!", totalKeys)
	} else {
		t.Fatalf("❌ FAIL — Lost %d keys during Chaos test!", missing)
	}
}
