package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"granite/internal/config"
	"granite/internal/raft"
	"granite/internal/rpc"
	"granite/internal/storage"
)

func main() {

	phase1 := flag.Bool("phase1", false, "run storage validation test")
	verifyOnly := flag.Bool("verify-only", false, "skip writes, only verify existing data")
	numKeys := flag.Int("keys", 10000, "number of keys to write")

	nodeID := flag.String("id", "", "unique identifier for cluster node mode (e.g. node1)")
	addr := flag.String("addr", "", "gRPC listen address (e.g. :5001)")
	peersFlag := flag.String("peers", "", "comma-separated peers in format id=addr (e.g. node2=127.0.0.1:5002,node3=127.0.0.1:5003)")
	dataDir := flag.String("data", "./data", "directory to store WAL and SSTable files")

	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if *nodeID == "" || *phase1 || *verifyOnly {
		runStorageTest(*dataDir, *verifyOnly, *numKeys)
		return
	}

	peerAddrs := []string{}
	peerMap := map[string]string{}
	if *peersFlag != "" {
		for _, spec := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(strings.TrimSpace(spec), "=", 2)
			if len(parts) == 2 {
				peerMap[parts[0]] = parts[1]
				peerAddrs = append(peerAddrs, parts[1])
			}
		}
	}

	cfg := config.DefaultConfig()
	cfg.NodeID = *nodeID
	cfg.ListenAddr = *addr
	cfg.Peers = peerAddrs
	cfg.DataDir = *dataDir

	slog.Info("Starting GRANITE cluster node",
		"id", cfg.NodeID,
		"addr", cfg.ListenAddr,
		"peers", peerAddrs,
		"data_dir", cfg.DataDir,
	)

	engine, err := storage.Open(cfg.DataDir)
	if err != nil {
		slog.Error("Failed to open storage engine", "error", err)
		os.Exit(1)
	}

	applyCh := make(chan raft.ApplyMsg, 256)

	pool := rpc.NewPeerPool(cfg.NodeID)
	for peerID, peerAddr := range peerMap {
		if err := pool.Connect(peerID, peerAddr); err != nil {
			slog.Warn("Could not pre-connect to peer (will retry on first RPC)",
				"peer", peerID, "addr", peerAddr, "error", err)
		}
	}

	raftNode, err := raft.NewNode(cfg, pool, applyCh)
	if err != nil {
		slog.Error("Failed to create Raft node", "error", err)
		os.Exit(1)
	}

	sm := raft.NewStateMachine(engine, applyCh)
	sm.Start()

	grpcServer := rpc.NewGRPCServer(cfg.NodeID, raftNode, engine)
	go func() {
		if err := grpcServer.Start(cfg.ListenAddr); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	raftNode.Start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down node...", "id", cfg.NodeID)
	raftNode.Stop()
	grpcServer.Stop()
	sm.Stop()
	pool.Close()
	if err := engine.Close(); err != nil {
		slog.Error("Engine close error", "error", err)
	}
	slog.Info("Node stopped cleanly.", "id", cfg.NodeID)
}

func runStorageTest(dataDir string, verifyOnly bool, numKeys int) {
	slog.Info("GRANITE Storage Engine Validation Starting",
		"data_dir", dataDir,
		"verify_only", verifyOnly,
		"num_keys", numKeys,
	)

	start := time.Now()
	engine, err := storage.Open(dataDir)
	if err != nil {
		slog.Error("Failed to open storage engine", "error", err)
		os.Exit(1)
	}
	slog.Info("Engine opened", "elapsed", time.Since(start))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("Signal received, shutting down")
		_ = engine.Close()
		os.Exit(0)
	}()

	if !verifyOnly {
		fmt.Printf("\n📝 Writing %d key-value pairs...\n", numKeys)
		writeStart := time.Now()

		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key_%04d", i))
			value := []byte(fmt.Sprintf("value_%04d", i))

			if err := engine.Put(key, value); err != nil {
				slog.Error("Put failed", "key", string(key), "error", err)
				os.Exit(1)
			}

			if (i+1)%1000 == 0 {
				fmt.Printf("  ✓ Written %d / %d keys\n", i+1, numKeys)
			}
		}

		writeDuration := time.Since(writeStart)
		opsPerSec := float64(numKeys) / writeDuration.Seconds()
		fmt.Printf("\n✅ Write complete: %d keys in %v\n", numKeys, writeDuration)
		fmt.Printf("   Throughput: %.0f writes/second\n\n", opsPerSec)
	}

	fmt.Printf("🔍 Verifying all %d keys...\n", numKeys)
	verifyStart := time.Now()

	missingCount := 0
	wrongCount := 0

	deletedKeys := map[string]bool{
		"key_0000": verifyOnly,
		"key_0001": verifyOnly,
		"key_0002": verifyOnly,
	}

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("value_%04d", i))

		if deletedKeys[string(key)] {
			continue
		}

		val, err := engine.Get(key)
		if err != nil {
			missingCount++
			if missingCount <= 10 {
				slog.Warn("Key missing!", "key", string(key), "error", err)
			}
			continue
		}

		if string(val) != string(expectedValue) {
			wrongCount++
			if wrongCount <= 10 {
				slog.Warn("Value mismatch!", "key", string(key),
					"expected", string(expectedValue), "got", string(val))
			}
		}
	}

	verifyDuration := time.Since(verifyStart)

	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  GRANITE Storage Engine — Validation Results")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Total keys expected : %d\n", numKeys)
	fmt.Printf("  Missing keys        : %d\n", missingCount)
	fmt.Printf("  Wrong values        : %d\n", wrongCount)
	fmt.Printf("  Read time           : %v\n", verifyDuration)
	fmt.Println()

	if missingCount == 0 && wrongCount == 0 {
		fmt.Println("  ✅ PASS — All data is correct and recoverable!")
	} else {
		fmt.Println("  ❌ FAIL — Data loss or corruption detected!")
	}
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	if !verifyOnly {
		fmt.Println("🗑️  Testing Delete (deleting key_0000, key_0001, key_0002)...")
		for _, k := range []string{"key_0000", "key_0001", "key_0002"} {
			if err := engine.Delete([]byte(k)); err != nil {
				slog.Error("Delete failed", "key", k, "error", err)
			}
		}
		for _, k := range []string{"key_0000", "key_0001", "key_0002"} {
			_, err := engine.Get([]byte(k))
			if err == storage.ErrNotFound {
				fmt.Printf("  ✅ key %q correctly returns ErrNotFound after delete\n", k)
			} else if err == nil {
				fmt.Printf("  ❌ key %q still exists after delete!\n", k)
			} else {
				fmt.Printf("  ❌ key %q unexpected error: %v\n", k, err)
			}
		}
		fmt.Println()
	}

	_ = engine.Close()
	slog.Info("Done. Total runtime", "elapsed", time.Since(start))
}
