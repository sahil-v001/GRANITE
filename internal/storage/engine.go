package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")

const memTableMaxSize int64 = 64 * 1024 * 1024

type Engine struct {
	mu        sync.RWMutex
	dataDir   string
	wal       *WAL
	mem       *MemTable
	sstables  []*SSTableReader
	compactor *Compactor
	cancelCtx context.CancelFunc
}

func Open(dataDir string) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: failed to create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "wal.log")

	slog.Info("engine: starting recovery", "data_dir", dataDir)
	mem, recordsReplayed, err := Recover(walPath)
	if err != nil {
		return nil, fmt.Errorf("engine: recovery failed: %w", err)
	}
	slog.Info("engine: recovery complete", "records_replayed", recordsReplayed)

	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("engine: failed to open WAL: %w", err)
	}

	sstPaths, err := findSSTableFiles(dataDir)
	if err != nil {
		return nil, fmt.Errorf("engine: failed to list sstables: %w", err)
	}

	for i, j := 0, len(sstPaths)-1; i < j; i, j = i+1, j-1 {
		sstPaths[i], sstPaths[j] = sstPaths[j], sstPaths[i]
	}

	var sstables []*SSTableReader
	for _, p := range sstPaths {
		r, err := OpenSSTableReader(p)
		if err != nil {
			slog.Warn("engine: skipping corrupt sstable", "path", p, "error", err)
			continue
		}
		sstables = append(sstables, r)
	}
	slog.Info("engine: loaded sstable files", "count", len(sstables))

	ctx, cancel := context.WithCancel(context.Background())
	compactor := NewCompactor(dataDir, 30*time.Second)
	compactor.Start(ctx)

	e := &Engine{
		dataDir:   dataDir,
		wal:       wal,
		mem:       mem,
		sstables:  sstables,
		compactor: compactor,
		cancelCtx: cancel,
	}

	go e.processCompactionResults(ctx)

	slog.Info("engine: ready", "data_dir", dataDir)
	return e, nil
}

func (e *Engine) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("engine: key cannot be empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(OpPut, key, value); err != nil {
		return fmt.Errorf("engine: WAL append failed: %w", err)
	}

	e.mem.Put(key, value)

	e.maybeFlushLocked()

	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("engine: key cannot be empty")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if val, found := e.mem.Get(key); found {
		if val == nil {
			return nil, ErrNotFound
		}
		return val, nil
	}

	for _, sst := range e.sstables {
		val, found, err := sst.Get(key)
		if err != nil {
			slog.Warn("engine: SSTable read error", "path", sst.Path(), "error", err)
			continue
		}
		if found {
			if val == nil {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	return nil, ErrNotFound
}

func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("engine: key cannot be empty")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(OpDelete, key, nil); err != nil {
		return fmt.Errorf("engine: WAL append failed: %w", err)
	}

	e.mem.Delete(key)

	e.maybeFlushLocked()

	return nil
}

func (e *Engine) maybeFlushLocked() {
	if e.mem.Size() < memTableMaxSize {
		return
	}

	slog.Info("engine: MemTable full, starting flush",
		"size_bytes", e.mem.Size(),
		"entries", e.mem.Len(),
	)

	entries := e.mem.Entries()

	e.mem = NewMemTable()

	sstPath := filepath.Join(e.dataDir,
		fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano()))

	e.mu.Unlock()

	err := WriteSSTable(entries, sstPath)

	e.mu.Lock()

	if err != nil {
		slog.Error("engine: SSTable flush failed", "error", err)
		return
	}

	reader, err := OpenSSTableReader(sstPath)
	if err != nil {
		slog.Error("engine: failed to open new sstable reader", "error", err)
		return
	}

	e.sstables = append([]*SSTableReader{reader}, e.sstables...)

	slog.Info("engine: flush complete", "sstable", sstPath)
}

func (e *Engine) processCompactionResults(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-e.compactor.ResultCh():
			e.applyCompactionResult(result)
		}
	}
}

func (e *Engine) applyCompactionResult(result CompactionResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	removedSet := make(map[string]bool)
	for _, p := range result.RemovedPaths {
		removedSet[p] = true
	}

	var newList []*SSTableReader
	for _, sst := range e.sstables {
		if !removedSet[sst.Path()] {
			newList = append(newList, sst)
		}
	}

	if result.NewSSTablePath != "" {
		reader, err := OpenSSTableReader(result.NewSSTablePath)
		if err != nil {
			slog.Error("engine: failed to open compacted sstable", "error", err)
		} else {
			newList = append(newList, reader)
			sort.Slice(newList, func(i, j int) bool {
				return newList[i].Path() > newList[j].Path()
			})
		}
	}

	e.sstables = newList

	for _, p := range result.RemovedPaths {
		if err := os.Remove(p); err != nil {
			slog.Warn("engine: failed to delete old sstable", "path", p, "error", err)
		}
	}

	slog.Info("engine: compaction result applied",
		"new_sstable", result.NewSSTablePath,
		"removed", result.RemovedPaths,
		"total_sstables", len(e.sstables),
	)
}

func (e *Engine) Close() error {
	slog.Info("engine: shutting down")

	e.cancelCtx()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mem.Len() > 0 {
		slog.Info("engine: flushing MemTable on shutdown", "entries", e.mem.Len())
		entries := e.mem.Entries()
		sstPath := filepath.Join(e.dataDir,
			fmt.Sprintf("sstable_%d.sst", time.Now().UnixNano()))

		e.mu.Unlock()
		err := WriteSSTable(entries, sstPath)
		e.mu.Lock()

		if err != nil {
			slog.Error("engine: shutdown flush failed", "error", err)
		}
	}

	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("engine: WAL close failed: %w", err)
	}

	slog.Info("engine: shutdown complete")
	return nil
}