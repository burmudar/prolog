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

func (idx *index) Read(in int64) (idxPos uint32, storePos uint64, err error) {
	// if out size is zero the index is empty
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	// -1 get last entry in the index
	// > 0 get the entry
	entry := uint64(0)
	if in == -1 {
		// amount of entries = size / entry size
		entry = idx.size/uint64(entryWidth) - 1
	} else if in >= 0 {
		entry = uint64(in)
	} else {
		return 0, 0, fmt.Errorf("invalid index offset")
	}

	// in | start pos | size | entryWidth
	// 0  | 0         | 12   | 12
	// 1  | 12        | 24   | 12
	// 2  | 24        | 36   | 12
	// 3  | 36        | 48   | 12
	pos := entry * entryWidth
	if idx.size < pos+uint64(entryWidth) {
		return 0, 0, io.EOF
	}
	// now that we have the position, decode the binary
	// Remember! entryWidth consists of 12 bytes
	// [         12          ]
	// [idx(4)i32|store(8)i64]
	idxPos = enc.Uint32(idx.mmap[pos : pos+indexPosWidth])
	storePos = enc.Uint64(idx.mmap[pos+indexPosWidth : pos+entryWidth])
	return idxPos, storePos, nil
}

func (idx *index) Write(off, pos uint64) error {
	return nil
}
