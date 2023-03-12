package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestSingleStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "single_store_append_read_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)

	s, err := newStore(f)
	require.NoError(t, err)

	n, pos, err := s.Append(write)
	require.NoError(t, err)
	require.NotZero(t, n)
	require.Zero(t, pos)

	data, err := s.Read(pos)
	require.NoError(t, err)
	require.Equal(t, write, data)

	res := make([]byte, len(write))
	s.ReadAt(res, 0)
}

func TestMultipleStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "multi_store_append_read_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)

	s, err := newStore(f)
	require.NoError(t, err)

	// do multiple appends
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}

	// read out appends
	off := int64(0)
	for i := 1; i < 4; i++ {
		read, err := s.Read(uint64(off))
		require.NoError(t, err)
		require.Equal(t, write, read)

		off += int64(width)

	}

	// now read at specific positions with read at
	off = 0
	for i := 1; i < 4; i++ {
		// the length of the record is store first so we want to get that first
		b := make([]byte, lenWidth)
		// n is how many bytes we read
		n, err := s.ReadAt(b, int64(off))
		require.NoError(t, err)
		require.Equal(t, n, lenWidth)
		// we read the length so we advance the cursor
		off += int64(n)
		// decode the size
		size := enc.Uint64(b)
		r := make([]byte, size)
		// now we know how much of the record we need to read
		n, err = s.ReadAt(r, int64(off))
		require.NoError(t, err)
		require.Equal(t, write, r)
		require.Equal(t, int(size), n)
		//we read the record of n size, so move n positions ahead
		off += int64(n)
	}
}

func TestClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// we don't care about what we wrote just that something was written
	// the data isn't flushed yet
	_, _, err = s.Append(write)
	require.NoError(t, err)

	_, beforeSize, err := openFile(t, f.Name())
	require.NoError(t, err)

	// Close flushes the buffer
	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(t, f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)

}

func openFile(t *testing.T, name string) (*os.File, int64, error) {
	t.Helper()

	f, err := os.OpenFile(name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, info.Size(), nil
}
