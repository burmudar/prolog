package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	// defines the number of bytes used to store the record's length
	// http://golang.org/ref/spec#Size_and_alignment_guarantees
	//
	// Entry storage in file:
	// [record length - 8 bytes][     record      ]
	// [record length - 8 bytes][     record      ]
	recordLenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &store{
		File: f,
		mu:   sync.Mutex{},
		buf:  bufio.NewWriter(f),
		size: uint64(info.Size()),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// Write the length of the record using the binary encoding
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the record data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// append the size of the length we wrote first using binary.Write
	w += recordLenWidth

	// update the size so that we know where our next write should start at
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// First ensure all records have been written to disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// size is the byte array that will keep the size encoded in binary
	// recordLenWidth = the length of the binary array encoded size
	size := make([]byte, recordLenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// encode the binary of the size into it's uint64 representation and create a slice of that size
	record := make([]byte, enc.Uint64(size))
	// read into the record slice, adjust the pos with the record length so that we start reading AT the record
	if _, err := s.File.ReadAt(record, int64(pos+recordLenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.File.Close()
}
