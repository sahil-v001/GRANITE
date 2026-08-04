package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type WAL struct {
	file *os.File
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(data []byte) error {
	length   := uint32(len(data))
	checksum := crc32.ChecksumIEEE(data)

	var header [8]byte
	binary.BigEndian.PutUint32(header[0:4], length)
	binary.BigEndian.PutUint32(header[4:8], checksum)

	if _, err := w.file.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) ReadAll() ([][]byte, int64, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}

	var records [][]byte
	var validOffset int64

	for {
		var header [8]byte
		_, err := io.ReadFull(w.file, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, validOffset, err
		}

		length   := binary.BigEndian.Uint32(header[0:4])
		checksum := binary.BigEndian.Uint32(header[4:8])

		data := make([]byte, length)
		_, err = io.ReadFull(w.file, data)
		if err == io.ErrUnexpectedEOF {
			break 
		}
		if err != nil {
			return nil, validOffset, err
		}

		if crc32.ChecksumIEEE(data) != checksum {
			break
		}

		records = append(records, data)
		validOffset += int64(8 + length) 
	}

	return records, validOffset, nil
}

func (w *WAL) Truncate(size int64) error {
	return w.file.Truncate(size)
}


func (w *WAL) Close() error {
	return w.file.Close()
}