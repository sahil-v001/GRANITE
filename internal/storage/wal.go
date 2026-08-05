package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type OpType byte

const (
	OpPut OpType = 0x01
	OpDelete OpType = 0x02
)

// 4 (CRC32) + 1 (OpType) + 2 (KeyLen) + 4 (ValueLen) = 11 bytes
const walHeaderSize = 11

type WAL struct {
	file *os.File 
	path string  
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open file at %q: %w", path, err)
	}
	return &WAL{file: file, path: path}, nil
}

func (w *WAL) Append(op OpType, key, value []byte) error {
	if len(key) > 65535 {
		return fmt.Errorf("wal: key too large: %d bytes (max 65535)", len(key))
	}

	bodySize := 1 + 2 + 4 + len(key) + len(value) // OpType + KeyLen + ValueLen + key + value
	body := make([]byte, bodySize)
	offset := 0

	body[offset] = byte(op)
	offset += 1

	binary.LittleEndian.PutUint16(body[offset:], uint16(len(key)))
	offset += 2

	binary.LittleEndian.PutUint32(body[offset:], uint32(len(value)))
	offset += 4

	copy(body[offset:], key)
	offset += len(key)

	copy(body[offset:], value)

	checksum := crc32.ChecksumIEEE(body) 

	record := make([]byte, walHeaderSize+len(key)+len(value))
	binary.LittleEndian.PutUint32(record[0:4], checksum) 
	copy(record[4:], body)                              

	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("wal: failed to write record: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync to disk: %w", err)
	}

	return nil
}

func (w *WAL) Close() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: final sync failed: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: failed to close file: %w", err)
	}
	return nil
}

func (w *WAL) Path() string {
	return w.path
}

type WALRecord struct {
	Op    OpType 
	Key   []byte 
	Value []byte 
}

func ReadWALRecords(path string) ([]WALRecord, error) {
	file, err := os.Open(path) // Open for reading only
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: failed to open for recovery: %w", err)
	}
	defer file.Close()

	var records []WALRecord
	header := make([]byte, walHeaderSize) 

	for {
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("wal: error reading header: %w", err)
		}

		storedChecksum := binary.LittleEndian.Uint32(header[0:4]) 
		op := OpType(header[4])                                  
		keyLen := binary.LittleEndian.Uint16(header[5:7])     
		valueLen := binary.LittleEndian.Uint32(header[7:11])  

		bodySize := 1 + 2 + 4 + int(keyLen) + int(valueLen) 
		body := make([]byte, bodySize)

		body[0] = byte(op)
		binary.LittleEndian.PutUint16(body[1:3], keyLen)
		binary.LittleEndian.PutUint32(body[3:7], valueLen)

		_, err = io.ReadFull(file, body[7:])
		if err != nil {
			break
		}

		computedChecksum := crc32.ChecksumIEEE(body)
		if computedChecksum != storedChecksum {
			break
		}

		key := make([]byte, keyLen)
		copy(key, body[7:7+keyLen])

		var value []byte
		if valueLen > 0 {
			value = make([]byte, valueLen)
			copy(value, body[7+keyLen:])
		}

		records = append(records, WALRecord{Op: op, Key: key, Value: value})
	}

	return records, nil
}