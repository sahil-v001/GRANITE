package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const sparseIndexInterval = 16

const footerSize = 16

type ssRecord struct {
	key   []byte
	value []byte
	op    OpType
}

type indexEntry struct {
	key    []byte
	offset int64
}

func WriteSSTable(entries []MemTableEntry, path string) error {
	if len(entries) == 0 {
		return fmt.Errorf("sstable: cannot write empty SSTable")
	}

	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("sstable: failed to create temp file: %w", err)
	}

	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	var sparseIndex []indexEntry
	var currentOffset int64 = 0

	for i, entry := range entries {
		if i%sparseIndexInterval == 0 {
			sparseIndex = append(sparseIndex, indexEntry{
				key:    entry.Key,
				offset: currentOffset,
			})
		}

		recordSize := 2 + 4 + 1 + len(entry.Key) + len(entry.Value)
		record := make([]byte, recordSize)
		offset := 0

		binary.LittleEndian.PutUint16(record[offset:], uint16(len(entry.Key)))
		offset += 2

		binary.LittleEndian.PutUint32(record[offset:], uint32(len(entry.Value)))
		offset += 4

		record[offset] = byte(entry.Op)
		offset += 1

		copy(record[offset:], entry.Key)
		offset += len(entry.Key)

		copy(record[offset:], entry.Value)

		n, err := file.Write(record)
		if err != nil {
			return fmt.Errorf("sstable: failed to write data record: %w", err)
		}
		currentOffset += int64(n)
	}

	indexStartOffset := currentOffset

	for _, ie := range sparseIndex {
		entrySize := 2 + len(ie.key) + 8
		buf := make([]byte, entrySize)
		offset := 0

		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(ie.key)))
		offset += 2

		copy(buf[offset:], ie.key)
		offset += len(ie.key)

		binary.LittleEndian.PutUint64(buf[offset:], uint64(ie.offset))

		n, err := file.Write(buf)
		if err != nil {
			return fmt.Errorf("sstable: failed to write index entry: %w", err)
		}
		currentOffset += int64(n)
	}

	indexSize := currentOffset - indexStartOffset

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexStartOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexSize))

	if _, err := file.Write(footer); err != nil {
		return fmt.Errorf("sstable: failed to write footer: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sstable: failed to sync: %w", err)
	}
	file.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("sstable: failed to rename temp file: %w", err)
	}

	success = true
	return nil
}

type SSTableReader struct {
	path        string
	sparseIndex []indexEntry
}

func OpenSSTableReader(path string) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open %q: %w", path, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	if fileSize < footerSize {
		return nil, fmt.Errorf("sstable: file too small to be valid: %d bytes", fileSize)
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, fileSize-footerSize); err != nil {
		return nil, fmt.Errorf("sstable: failed to read footer: %w", err)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:16]))

	indexData := make([]byte, indexSize)
	if _, err := file.ReadAt(indexData, indexOffset); err != nil {
		return nil, fmt.Errorf("sstable: failed to read index: %w", err)
	}

	var sparseIndex []indexEntry
	pos := 0
	for pos < len(indexData) {
		if pos+2 > len(indexData) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(indexData[pos : pos+2]))
		pos += 2

		if pos+keyLen+8 > len(indexData) {
			break
		}
		key := make([]byte, keyLen)
		copy(key, indexData[pos:pos+keyLen])
		pos += keyLen

		offset := int64(binary.LittleEndian.Uint64(indexData[pos : pos+8]))
		pos += 8

		sparseIndex = append(sparseIndex, indexEntry{key: key, offset: offset})
	}

	return &SSTableReader{
		path:        path,
		sparseIndex: sparseIndex,
	}, nil
}

func (r *SSTableReader) Get(key []byte) (value []byte, found bool, err error) {
	if len(r.sparseIndex) == 0 {
		return nil, false, nil
	}

	lo, hi := 0, len(r.sparseIndex)-1
	startOffset := r.sparseIndex[0].offset

	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(r.sparseIndex[mid].key, key)
		if cmp <= 0 {
			startOffset = r.sparseIndex[mid].offset
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	file, err := os.Open(r.path)
	if err != nil {
		return nil, false, fmt.Errorf("sstable: failed to open for read: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("sstable: seek failed: %w", err)
	}

	header := make([]byte, 7)
	for {
		_, err := io.ReadFull(file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("sstable: read error: %w", err)
		}

		keyLen := int(binary.LittleEndian.Uint16(header[0:2]))
		valueLen := int(binary.LittleEndian.Uint32(header[2:6]))
		op := OpType(header[6])

		recordKey := make([]byte, keyLen)
		if _, err := io.ReadFull(file, recordKey); err != nil {
			break
		}

		cmp := bytes.Compare(recordKey, key)

		if cmp > 0 {
			break
		}

		recordValue := make([]byte, valueLen)
		if valueLen > 0 {
			if _, err := io.ReadFull(file, recordValue); err != nil {
				break
			}
		}

		if cmp == 0 {
			if op == OpDelete {
				return nil, true, nil
			}
			return recordValue, true, nil
		}
	}

	return nil, false, nil
}

func (r *SSTableReader) Path() string {
	return r.path
}

func (r *SSTableReader) Close() error {
	return nil
}