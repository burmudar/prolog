package log

import (
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	indexPosWidth uint64 = 4
	storePosWidth uint64 = 8
	entryWidth           = indexPosWidth + storePosWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

type Segment struct {
	MaxIndexBytes int64
}
type Config struct {
	Segment
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := index{
		file: f,
	}

	// get the size of the file before we "grow" it
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(info.Size())

	// now we grow the file
	if err := os.Truncate(idx.file.Name(), c.Segment.MaxIndexBytes); err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(
		f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return &idx, nil
}

func (idx *index) Close() error {
	// we need to make sure:
	// 1. map has flushed
	// 2. file is flushed
	// 3. ensure the file is the size we calculated and not the max grow size
	//
	// now we can close
	//
	// Flush the map
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Flush the file
	if err := idx.file.Sync(); err != nil {
		return err
	}

	if err := os.Truncate(idx.file.Name(), int64(idx.size)); err != nil {
		return err
	}

	// Close the file
	return idx.file.Close()
}

// Read takes in an offset and returns the associated position in the store
func (idx *index) Read(in int64) (idxPos uint32, storePos uint64, err error) {
	// if our size is zero the index is empty
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	// off is the record offset
	// -1 get last off in the index
	// > 0 get the off
	off := uint64(0)
	if in == -1 {
		// amount of entries = size / entry size
		totalEntries := idx.size / uint64(entryWidth)
		off = totalEntries - 1
	} else if in >= 0 {
		off = uint64(in)
	} else {
		return 0, 0, fmt.Errorf("invalid index offset")
	}

	// in | start pos | size | entryWidth
	// 0  | 0         | 12   | 12
	// 1  | 12        | 24   | 12
	// 2  | 24        | 36   | 12
	// 3  | 36        | 48   | 12
	// pos is the entry position
	pos := off * entryWidth
	// we can't read past the end of the file
	if idx.size < pos+uint64(entryWidth) {
		return 0, 0, io.EOF
	}
	// now that we have the position, decode the binary
	// Remember! entryWidth consists of 12 bytes
	// [         12          ]
	// [idx(4)u32|store(8)u64]
	idxPos = enc.Uint32(idx.mmap[pos : pos+indexPosWidth])
	storePos = enc.Uint64(idx.mmap[pos+indexPosWidth : pos+entryWidth])
	return idxPos, storePos, nil
}

// Write writes to the index
// off is the record offset
// pos is the position in the store file
func (idx *index) Write(off, pos uint64) error {
	// check that adding the entry won't make it larger than our file
	if uint64(len(idx.mmap)) < pos+entryWidth {
		return io.EOF
	}

	enc.PutUint32(idx.mmap[idx.size:idx.size+indexPosWidth], uint32(off))
	enc.PutUint64(idx.mmap[idx.size+indexPosWidth:idx.size+entryWidth], pos)
	// we need to increment the size so that we now where the next entry should start
	idx.size += uint64(entryWidth)
	return nil
}

func (idx *index) Name() string {
	return idx.file.Name()
}
