package storage

import (
	"path/filepath"
	"testing"
)

func TestWALAndRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	_ = wal.Append(OpPut, []byte("k1"), []byte("v1"))
	_ = wal.Append(OpPut, []byte("k2"), []byte("v2"))
	_ = wal.Append(OpDelete, []byte("k1"), nil)
	_ = wal.Close()

	mem, count, err := Recover(walPath)
	if err != nil {
		t.Fatalf("WAL recovery failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 records replayed, got %d", count)
	}

	val, found := mem.Get([]byte("k1"))
	if !found || val != nil {
		t.Errorf("Expected k1 to be tombstone (found=true, val=nil), got found=%v, val=%s", found, string(val))
	}

	val, found = mem.Get([]byte("k2"))
	if !found || string(val) != "v2" {
		t.Errorf("Expected k2='v2', got found=%v, val=%s", found, string(val))
	}
}

func TestMemTableSkipList(t *testing.T) {
	mem := NewMemTable()

	mem.Put([]byte("b"), []byte("val_b"))
	mem.Put([]byte("a"), []byte("val_a"))
	mem.Put([]byte("c"), []byte("val_c"))

	entries := mem.Entries()
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}

	if string(entries[0].Key) != "a" || string(entries[1].Key) != "b" || string(entries[2].Key) != "c" {
		t.Errorf("Entries out of sorted order: %v, %v, %v", string(entries[0].Key), string(entries[1].Key), string(entries[2].Key))
	}
}

func TestSSTableWriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "sstable_test.sst")

	entries := []MemTableEntry{
		{Key: []byte("alpha"), Value: []byte("val1"), Op: OpPut},
		{Key: []byte("beta"), Value: []byte("val2"), Op: OpPut},
		{Key: []byte("gamma"), Value: nil, Op: OpDelete},
	}

	if err := WriteSSTable(entries, sstPath); err != nil {
		t.Fatalf("Failed to write SSTable: %v", err)
	}

	reader, err := OpenSSTableReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to open SSTable reader: %v", err)
	}

	val, found, err := reader.Get([]byte("beta"))
	if err != nil || !found || string(val) != "val2" {
		t.Errorf("Expected beta='val2', got err=%v, found=%v, val=%s", err, found, string(val))
	}

	val, found, err = reader.Get([]byte("gamma"))
	if err != nil || !found || val != nil {
		t.Errorf("Expected gamma tombstone, got err=%v, found=%v, val=%s", err, found, string(val))
	}
}
