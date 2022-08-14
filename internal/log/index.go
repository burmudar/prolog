package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// An entry in the index consists of two parts
// <[ off width ][ pos width ]>
// <[ off width ][ pos width ]>

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// size the file to the max allow sized - essentially creating a "sparse index"
	// we have to grow the file before hand since we can't do it when the file is memory mapped
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// here the file is memory mapped after the file has been grown to it's max size
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// if the index is empty, we have nothing to return
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// Get the last entry ?
	if in == -1 {
		out = uint32(i.size/entWidth - 1)
	} else {
		out = uint32(in)
	}

	// Get starting position of entry
	pos = uint64(out) * entWidth
	// if our intended entry is past the size of the index, we don't have it
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// the position in the store file
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// Check if we have space to write the entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// store the offset. i.size is the start position for our index entry
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// store the position in the store file after the offset
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// increment the size, so that the next write goes to the write position
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// Closing happens in three stages:
	// 1. Sync the memory contents to file
	// 2. Sync the file to storage
	// 3. Shrink the file to it's ACTUAL size
	// finally close the file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := os.Truncate(i.file.Name(), int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}
