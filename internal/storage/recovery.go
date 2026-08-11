package storage

import (
	"fmt"
	"log/slog"
	"os"
)

func Recover(walPath string) (*MemTable, int, error) {
	slog.Info("starting WAL recovery", "path", walPath)

	records, err := ReadWALRecords(walPath)
	if err != nil {
		return nil, 0, fmt.Errorf("recovery: failed to read WAL: %w", err)
	}

	mem := NewMemTable()
	count := 0

	for _, rec := range records {
		switch rec.Op {
		case OpPut:

			mem.Put(rec.Key, rec.Value)
			count++

		case OpDelete:

			mem.Delete(rec.Key)
			count++

		default:

			slog.Warn("recovery: unknown OpType encountered, skipping",
				"op", rec.Op)
		}
	}

	slog.Info("WAL recovery complete",
		"records_replayed", count,
		"memtable_entries", mem.Len(),
		"memtable_size_bytes", mem.Size(),
	)

	if _, statErr := os.Stat(walPath); statErr == nil {

		validBytes, err := computeValidWALSize(walPath, len(records))
		if err != nil {
			slog.Warn("recovery: could not compute valid WAL size, skipping truncation",
				"error", err)
		} else {
			if truncErr := truncateWAL(walPath, validBytes); truncErr != nil {
				slog.Warn("recovery: WAL truncation failed", "error", truncErr)
			} else {
				slog.Info("WAL truncated to last valid record", "valid_bytes", validBytes)
			}
		}
	}

	return mem, count, nil
}

func computeValidWALSize(walPath string, validRecordCount int) (int64, error) {
	if validRecordCount == 0 {
		return 0, nil
	}

	records, err := ReadWALRecords(walPath)
	if err != nil {
		return 0, err
	}

	var totalBytes int64
	for i := 0; i < validRecordCount && i < len(records); i++ {
		rec := records[i]

		totalBytes += int64(walHeaderSize + len(rec.Key) + len(rec.Value))
	}
	return totalBytes, nil
}

func truncateWAL(walPath string, size int64) error {

	if err := os.Truncate(walPath, size); err != nil {
		return fmt.Errorf("recovery: os.Truncate failed: %w", err)
	}
	return nil
}

