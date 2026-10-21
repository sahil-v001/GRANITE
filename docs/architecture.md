# 🪨 GRANITE Architecture & Deep-Dive Design Document

GRANITE is a production-grade, distributed, fault-tolerant key-value storage engine built from scratch in Go.
It translates theoretical distributed storage concepts from **Martin Kleppmann’s *Designing Data-Intensive Applications* (DDIA)** into functional Go code.

---

## 1. System Architecture

```text
                                CLIENT
                       PUT / GET / DELETE
                               │
                               ▼
                   ┌──────────────────────┐
                   │  Raft Leader Node    │
                   └──────────┬───────────┘
                              │
               AppendEntries  │  AppendEntries
               (gRPC / TCP)   │  (gRPC / TCP)
                      ┌───────┴───────┐
                      ▼               ▼
               ┌────────────┐   ┌────────────┐
               │ Follower 1 │   │ Follower 2 │
               └────────────┘   └────────────┘
                      │               │
                      └───────┬───────┘
                              │ 2/3 Quorum ACKs
                              ▼
                   ┌──────────────────────┐
                   │ Mark Entry Committed │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │  State Machine Apply │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │  LSM Storage Engine  │
                   └──────────┬───────────┘
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
   ┌───────────────────┐             ┌───────────────────┐
   │  WAL (wal.log)    │             │ MemTable (RAM)    │
   │  os.O_APPEND      │             │ Skip List         │
   │  + file.Sync()    │             │ + sync.RWMutex    │
   └───────────────────┘             └─────────┬─────────┘
                                               │ Flush (64MB)
                                               ▼
                                     ┌───────────────────┐
                                     │  SSTable (.sst)   │
                                     │  Sorted Binary    │
                                     │  + Sparse Index   │
                                     └───────────────────┘
```

---

## 2. Storage Layer — LSM-Tree (DDIA Chapter 3)

### Write-Ahead Log (WAL)
- **Format**: `[CRC32: 4B] [OpType: 1B] [KeyLen: 2B] [ValueLen: 4B] [Key] [Value]`
- **Durability**: Opened with `os.O_APPEND`. Calls `file.Sync()` (`fsync` system call) on every write.
- **Crash Recovery**: Reads WAL records sequentially, verifies CRC32 checksums, and truncates partial records at the crash boundary.

### MemTable (Skip List)
- **Data Structure**: Multi-level randomized Skip List with 50% elevation probability per level (`maxSkipListLevel = 16`).
- **Concurrency**: Protected by `sync.RWMutex` (`RLock` for concurrent reads, `Lock` for exclusive writes).
- **Sorted Flushes**: Keeping entries sorted in memory allows `O(N)` sequential disk dumps during SSTable flushes.

### SSTables (Sorted String Tables)
- **Format**: Immutable binary file containing sorted key-value data, a sparse index (every 16th key), and a 16-byte fixed footer (`[IndexOffset: 8B][IndexSize: 8B]`).
- **Atomic Writes**: Written to `.tmp` files first, followed by POSIX atomic `os.Rename`.

### Compaction
- **Strategy**: Background goroutine size-tiered compaction merging multiple SSTables into a single file.
- **Tombstones**: Discards deleted markers (tombstones) and older key versions during merge.

---

## 3. Distributed Consensus — Raft (DDIA Chapter 9)

### Roles & State Transitions
- Nodes transition between `Follower`, `Candidate`, and `Leader`.
- Elections trigger on randomized timeouts (150ms–300ms) to prevent split votes.
- Leaders send heartbeats every 50ms over gRPC.

### Log Replication & Quorum
- Writes are appended to the Leader's Raft log and sent to Followers via `AppendEntries`.
- Once replicated on a majority of nodes ($Q = \lfloor N/2 \rfloor + 1 = 2$), the entry is marked `COMMITTED` and applied to the LSM storage engine.

---

## 4. Key Performance Metrics

- **Write Latency (with fsync)**: P50 = 2.15ms, P95 = 2.74ms, P99 = 3.80ms
- **Write Throughput**: ~473 writes/sec (sequential hardware fsync bound)
- **Read Throughput**: ~3,800,000 reads/sec (MemTable RAM cache)
