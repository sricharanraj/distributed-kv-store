package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// opCode identifies the kind of mutation recorded in the WAL.
type opCode byte

const (
	opPut opCode = iota + 1
	opDelete
)

// WAL is an append-only write-ahead log used to recover the memtable after
// a crash. Every mutation is written and fsynced here before it is applied
// to the in-memory skip list, giving durability without waiting for a full
// SSTable flush.
//
// Record layout (little-endian):
//
//	[1 byte opCode][4 byte keyLen][key][4 byte valLen][value]
type WAL struct {
	file *os.File
	w    *bufio.Writer
}

func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: f, w: bufio.NewWriter(f)}, nil
}

func (w *WAL) append(op opCode, key string, value []byte) error {
	buf := make([]byte, 0, 9+len(key)+len(value))
	buf = append(buf, byte(op))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(key)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, key...)
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(value)))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, value...)

	if _, err := w.w.Write(buf); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// replay reads every record in the log from the start and invokes fn for each.
func replayWAL(path string, fn func(op opCode, key string, value []byte)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		opByte, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			// Truncated tail record (e.g. crash mid-write); stop replay.
			return nil
		}
		klen := binary.LittleEndian.Uint32(lenBuf[:])
		key := make([]byte, klen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil
		}
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return nil
		}
		vlen := binary.LittleEndian.Uint32(lenBuf[:])
		val := make([]byte, vlen)
		if _, err := io.ReadFull(r, val); err != nil {
			return nil
		}
		fn(opCode(opByte), string(key), val)
	}
}

func (w *WAL) truncate() error {
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}

func (w *WAL) close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
