package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc is the encoding we will use to store record and index entries in
var enc = binary.BigEndian

const (
	// lenWidth defines the number of bytes to store the records length
	lenWidth = 8
)

// store is a simple wrapper to append and read bytes to and from a file
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// check if we're restoring from an old file - for example if our service got restarted
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// we start at the end of the file, thus this is is the starting pos
	// of our record
	pos = s.size
	// first write the length of the p, so that we know how much to read
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// write the data of p
	// w = how many bytes were written
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// add the record width
	w += lenWidth
	// add the record width to the size so that we know where to start next,
	// and not overwrite the previous record
	s.size += uint64(w)

	// return:
	//	w = how many bytes were written
	//	pos = the position of the record
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// we flush first to make sure there is nothing still waiting to be written to disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// we read starting at pos, the record length in uint64
	// we first read the size of the record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// now that we have the record size we can read the record
	data := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return data, nil
}

// ReadAt satisfies the io.ReadAt interface
func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// make sure before we close the file
	// that all data has been written to file!
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()

}
