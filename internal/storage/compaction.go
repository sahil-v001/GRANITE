package storage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type CompactionResult struct {

	NewSSTablePath string

	RemovedPaths []string
}

type Compactor struct {
	dataDir  string
	interval time.Duration

	resultCh chan CompactionResult
}

func NewCompactor(dataDir string, interval time.Duration) *Compactor {
	return &Compactor{
		dataDir:  dataDir,
		interval: interval,
		resultCh: make(chan CompactionResult, 4),
	}
}

func (c *Compactor) ResultCh() <-chan CompactionResult {
	return c.resultCh
}

func (c *Compactor) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		slog.Info("compactor started", "interval", c.interval, "data_dir", c.dataDir)

		for {
			select {
			case <-ctx.Done():
				slog.Info("compactor stopping")
				return
			case <-ticker.C:
				if err := c.maybeCompact(); err != nil {
					slog.Error("compaction failed", "error", err)

				}
			}
		}
	}()
}

func (c *Compactor) maybeCompact() error {

	paths, err := findSSTableFiles(c.dataDir)
	if err != nil {
		return fmt.Errorf("compactor: failed to list sstables: %w", err)
	}

	const compactionThreshold = 4
	if len(paths) < compactionThreshold {
		return nil
	}

	slog.Info("starting compaction",
		"sstable_count", len(paths),
		"files", paths,
	)

	toCompact := paths[:2]

	result, err := compactFiles(toCompact, c.dataDir)
	if err != nil {
		return fmt.Errorf("compactor: merge failed: %w", err)
	}

	c.resultCh <- *result

	slog.Info("compaction complete",
		"merged_into", result.NewSSTablePath,
		"removed", result.RemovedPaths,
	)

	return nil
}

func compactFiles(paths []string, dataDir string) (*CompactionResult, error) {

	readers := make([]*SSTableReader, len(paths))
	for i, p := range paths {
		r, err := OpenSSTableReader(p)
		if err != nil {
			return nil, fmt.Errorf("compaction: failed to open %q: %w", p, err)
		}
		readers[i] = r
	}

	type versionedEntry struct {
		key      []byte
		value    []byte
		op       OpType
		fileIdx  int
	}

	var allEntries []versionedEntry

	for fileIdx, r := range readers {

		entries, err := readAllSSTableRecords(r.path)
		if err != nil {
			return nil, fmt.Errorf("compaction: failed to read %q: %w", r.path, err)
		}
		for _, e := range entries {
			allEntries = append(allEntries, versionedEntry{
				key:     e.key,
				value:   e.value,
				op:      e.op,
				fileIdx: fileIdx,
			})
		}
	}

	sort.SliceStable(allEntries, func(i, j int) bool {
		cmp := bytes.Compare(allEntries[i].key, allEntries[j].key)
		if cmp != 0 {
			return cmp < 0
		}
		return allEntries[i].fileIdx > allEntries[j].fileIdx
	})

	var merged []MemTableEntry
	var lastKey []byte

	for _, e := range allEntries {
		if lastKey != nil && bytes.Equal(e.key, lastKey) {
			continue
		}
		lastKey = e.key

		if e.op == OpDelete {

			continue
		}

		merged = append(merged, MemTableEntry{
			Key:   e.key,
			Value: e.value,
			Op:    e.op,
		})
	}

	if len(merged) == 0 {

		return &CompactionResult{
			NewSSTablePath: "",
			RemovedPaths:   paths,
		}, nil
	}

	newPath := filepath.Join(dataDir, fmt.Sprintf("sstable_%d_compacted.sst", time.Now().UnixNano()))
	if err := WriteSSTable(merged, newPath); err != nil {
		return nil, fmt.Errorf("compaction: failed to write merged sstable: %w", err)
	}

	return &CompactionResult{
		NewSSTablePath: newPath,
		RemovedPaths:   paths,
	}, nil
}

func readAllSSTableRecords(path string) ([]ssRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < footerSize {
		return nil, fmt.Errorf("file too small: %d bytes", fileSize)
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, fileSize-footerSize); err != nil {
		return nil, err
	}
	indexOffset := int64(bytesToUint64(footer[0:8]))

	var records []ssRecord
	var pos int64

	headerBuf := make([]byte, 7)

	for pos < indexOffset {
		n, err := file.ReadAt(headerBuf, pos)
		if n < 7 || err != nil {
			break
		}
		pos += 7

		keyLen := int(bytesToUint16(headerBuf[0:2]))
		valueLen := int(bytesToUint32(headerBuf[2:6]))
		op := OpType(headerBuf[6])

		key := make([]byte, keyLen)
		if _, err := file.ReadAt(key, pos); err != nil {
			break
		}
		pos += int64(keyLen)

		var value []byte
		if valueLen > 0 {
			value = make([]byte, valueLen)
			if _, err := file.ReadAt(value, pos); err != nil {
				break
			}
			pos += int64(valueLen)
		}

		records = append(records, ssRecord{key: key, value: value, op: op})
	}

	return records, nil
}

func findSSTableFiles(dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sst" {

			paths = append(paths, filepath.Join(dataDir, e.Name()))
		}
	}

	sort.Strings(paths)
	return paths, nil
}

func bytesToUint16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func bytesToUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func bytesToUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

