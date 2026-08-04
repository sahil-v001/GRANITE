package storage

import (
	"os"
	"testing"
)

func TestWALAppendAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "wal_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	records := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("granite"),
	}
	for _, r := range records {
		if err := wal.Append(r); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	wal.Close()

	wal2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("NewWAL (reopen) failed: %v", err)
	}
	defer wal2.Close()

	got, _, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}

	for i, r := range records {
		if string(got[i]) != string(r) {
			t.Errorf("record %d: expected %q, got %q", i, r, got[i])
		}
	}
}

func TestWALCrashRecovery(t *testing.T) {
	f, err := os.CreateTemp("", "wal_crash_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	_ = wal.Append([]byte("record-one"))
	_ = wal.Append([]byte("record-two"))
	wal.Close()

	file, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	file.Write([]byte{0x00, 0x00, 0x00, 0x05, 0xFF})
	file.Close()

	wal2, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	got, validOffset, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 records after crash recovery, got %d", len(got))
	}
	if string(got[0]) != "record-one" {
		t.Errorf("record 0: expected 'record-one', got %q", got[0])
	}
	if string(got[1]) != "record-two" {
		t.Errorf("record 1: expected 'record-two', got %q", got[1])
	}

	if err := wal2.Truncate(validOffset); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
}

func TestWALEmptyFile(t *testing.T) {
	f, err := os.CreateTemp("", "wal_empty_*.log")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	f.Close()
	defer os.Remove(path)

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	got, _, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll on empty WAL failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 records, got %d", len(got))
	}
}