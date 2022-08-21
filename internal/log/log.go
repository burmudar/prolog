package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/burmudar/prolog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func newLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offstr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offstr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// We want the segment ordering to be from oldest to newest, which is why we sort the offsets here
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsets contains offsets for indexes as well as stores. So we skip every second one
		i++
	}
	// in case no previous segments were created - we create one now!
	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

// newSegment creates a new segment with the given offsent and appends it to the log segments. The newly created Segment
// is also set to be the current active segment
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	if l.segments == nil {
		l.segments = make([]*segment, 0)
	}

	l.segments = append(l.segments)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// findSegment Finds a segment which contains the given offset
func (l *Log) findSegment(off uint64) *segment {
	for _, seg := range l.segments {
		if seg.baseOffset <= off && off < seg.nextOffset {
			return seg
		}
	}
	return nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	seg := l.findSegment(off)
	if seg == nil || seg.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return seg.Read(off)
}

// Close closes all the segments
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Remove closes the log and removes all the files used by the log
func (l *Log) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// Reset removes the log and its associated files and creates a new log
func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Truncate removes all segments whose highest offset is lower than the lowest
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest-1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments
	return nil
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.store.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// Reader allows us to read the entire log by concactenating all the segment stores in order.
// Each store is wrapped in an originReader to (a) satisfy the Reader interface and (b) allow us to keep track
// of the offset and start at the appropriate offset when going to the next store. With originReader satisfying the
// reader interface, all the readers can be passed into a MultiReader
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originReader{s.store, 0}
	}

	return io.MultiReader(readers...)
}
