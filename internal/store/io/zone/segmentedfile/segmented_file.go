// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segmentedfile

import (
	// standard libraries.
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/io/zone"
	"github.com/linkall-labs/vanus/observability/log"
)

type SegmentedFile struct {
	segments []*Segment
	mu       sync.RWMutex

	dir         string
	ext         string
	segmentSize int64
}

// Make sure file implements zone.Interface.
var _ zone.Interface = (*SegmentedFile)(nil)

func Open(dir string, opts ...Option) (*SegmentedFile, error) {
	cfg := makeConfig(opts...)

	segments, err := recoverSegments(dir, cfg)
	if err != nil {
		return nil, err
	}

	sf := &SegmentedFile{
		segments:    segments,
		dir:         dir,
		ext:         cfg.ext,
		segmentSize: cfg.segmentSize,
	}
	return sf, nil
}

func (sf *SegmentedFile) Close() {
	for _, s := range sf.segments {
		if err := s.Close(); err != nil {
			log.Error(context.Background(), "Close segment failed.", map[string]interface{}{
				"path":       s.path,
				log.KeyError: err,
			})
		}
	}
}

func (sf *SegmentedFile) Raw(off int64) (*os.File, int64) {
	s := sf.SelectSegment(off, true)
	if s == nil {
		return nil, 0
	}
	return s.f, off - s.so
}

func (sf *SegmentedFile) SelectSegment(offset int64, autoCreate bool) *Segment {
	sf.mu.RLock()

	sz := len(sf.segments)
	if sz == 0 {
		sf.mu.RUnlock()

		if offset == 0 {
			if !autoCreate {
				return nil
			}
			return sf.createNextSegment(nil)
		}
		panic("log stream not begin from 0")
	}

	// Fast return for append.
	if last := sf.lastSegment(); offset >= last.so {
		sf.mu.RUnlock()

		if offset < last.eo {
			return last
		}
		if offset == last.eo {
			if !autoCreate {
				return nil
			}
			return sf.createNextSegment(last)
		}
		panic("file segment overflow")
	}

	defer sf.mu.RUnlock()

	first := sf.firstSegment()
	if offset < first.so {
		panic("file segment underflow")
	}

	i := sort.Search(sz-1, func(i int) bool {
		return sf.segments[i].eo > offset
	})
	if i < sz-1 {
		return sf.segments[i]
	}

	panic("unreachable")
}

func (sf *SegmentedFile) createNextSegment(last *Segment) *Segment {
	var off int64
	if last != nil {
		off = last.eo
	}

	next, err := createSegment(sf.dir, sf.ext, off, sf.segmentSize, true)
	if err != nil {
		panic(err)
	}

	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.segments = append(sf.segments, next)

	return next
}

func createSegment(dir, ext string, so, size int64, sync bool) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d%s", so, ext))
	f, err := io.CreateFile(path, size, true, sync)
	if err != nil {
		return nil, err
	}
	return newSegment(path, so, size, f), nil
}

func (sf *SegmentedFile) firstSegment() *Segment {
	return sf.segments[0]
}

func (sf *SegmentedFile) lastSegment() *Segment {
	return sf.segments[len(sf.segments)-1]
}

func (sf *SegmentedFile) Dir() string {
	return sf.dir
}

func (sf *SegmentedFile) Len() int {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return len(sf.segments)
}
