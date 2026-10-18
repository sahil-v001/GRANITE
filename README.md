# 🪨 GRANITE — Distributed LSM-Tree Database in Go

[![Go](https://img.shields.io/badge/Go-1.22%2B-blue.svg)](https://golang.org)
[![Consensus](https://img.shields.io/badge/Consensus-Raft-red.svg)](https://raft.github.io)
[![Storage](https://img.shields.io/badge/Storage-LSM--Tree-green.svg)](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
[![RPC](https://img.shields.io/badge/RPC-gRPC%20%2F%20Protobuf-orange.svg)](https://grpc.io)

**GRANITE** is a production-grade, fault-tolerant, consensus-backed distributed key-value storage engine built from scratch in Go. It implements the core storage and distributed systems principles from Martin Kleppmann’s *Designing Data-Intensive Applications* (DDIA):

- **DDIA Chapter 3**: Storage & Retrieval (LSM-Tree, WAL, MemTable Skip List, SSTables, Compaction)
- **DDIA Chapter 5**: Replication (Leader-Follower log distribution over gRPC)
- **DDIA Chapter 9**: Consistency & Consensus (Raft leader election, quorum commits, crash recovery)

---

## 🏛️ Architecture Overview

```text
[ Client Write ] ──► [ Raft Leader ] ──(gRPC AppendEntries)──► [ Followers (2/3 Quorum) ]
                            │                                            │
                            ▼ (Committed)                                ▼ (Committed)
                  [ State Machine ]                            [ State Machine ]
                            │                                            │
                    [ LSM Engine ]                               [ LSM Engine ]
                   ┌────────┴────────┐                          ┌────────┴────────┐
                   ▼                 ▼                          ▼                 ▼
               [  WAL  ]        [ MemTable ]                [  WAL  ]        [ MemTable ]
              (O_APPEND          (Skip List                 (O_APPEND          (Skip List
              + fsync)           + RWMutex)                 + fsync)           + RWMutex)
                                     │                                             │
                                     ▼ (64MB)                                      ▼ (64MB)
                               [ SSTable (.sst) ]                            [ SSTable (.sst) ]
```

---

## ⚡ Key Technical Features

### 1. Storage Engine (LSM-Tree)
- **Write-Ahead Log (WAL)**: Binary packed `[CRC32:4B][OpType:1B][KeyLen:2B][ValueLen:4B][Key][Value]`. Synchronized to physical disk via `file.Sync()` (`fsync`) for crash safety.
- **MemTable**: Thread-safe multi-level Skip List using `sync.RWMutex`.
- **SSTables**: Immutable sorted string tables with sparse indexing (every 16th key) and atomic POSIX renaming.
- **Compaction**: Background goroutine size-tiered merge routines purging tombstones and stale entries.

### 2. Consensus Layer (Raft Protocol)
- **Leader Election**: Randomized election timers (150ms–300ms) preventing split votes.
- **Log Replication**: 50ms heartbeat intervals, log matching consistency checks, and automatic fallback retry.
- **Fault Tolerance**: Automatic leader failover and state machine recovery across node process crashes.
- **Snapshots**: Disk snapshot checkpoints (`snapshot.bin`) to bound Raft log growth.

---

## 📊 Benchmark Results

Measured on local disk with hardware `fsync` enabled:

| Operation | Throughput | P50 Latency | P95 Latency | P99 Latency |
|---|---|---|---|---|
| **Write (WAL fsync + MemTable)** | **~473 ops/sec** | **2.15 ms** | **2.74 ms** | **3.80 ms** |
| **Read (MemTable RAM Cache)** | **~3,878,000 ops/sec** | **< 1 µs** | **< 1 µs** | **< 1 µs** |

---

## 🛠️ Quickstart

### Prerequisites
- Go 1.22+
- `protoc` (Protocol Buffers compiler)

### Building & Running

```bash
# 1. Compile protocol buffers
make proto

# 2. Run all unit, integration, and Raft failover tests
make test

# 3. Run performance benchmarks
make bench

# 4. Spin up 3-node cluster (PowerShell)
.\scripts\start-cluster.ps1
```

---

## 📁 Repository Structure

```text
granite/
├── cmd/
│   ├── server/          # Cluster node entry point
│   └── benchmark/       # Performance benchmark CLI
├── internal/
│   ├── storage/         # LSM Engine (WAL, MemTable, SSTable, Recovery, Compaction)
│   ├── raft/            # Raft Consensus (Node, Election, Replication, Snapshot, State Machine)
│   ├── rpc/             # gRPC Server & Peer Client Pool
│   └── config/          # Node & Cluster Topologies
├── proto/               # Protobuf (.proto) definitions & generated Go stubs
├── docs/                # Architecture design documents
├── scripts/             # Cluster orchestration scripts
└── Makefile             # Build automation
```

---

## 📜 License
MIT License. Free for educational and interview presentation use.
