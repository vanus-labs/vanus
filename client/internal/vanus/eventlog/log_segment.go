// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventlog

import (
	// standard libraries
	"context"
	"encoding/binary"
	"math"
	"sync"

	// third-party libraries
	ce "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/atomic"

	// this project
	vdr "github.com/linkall-labs/vanus/client/internal/vanus/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

func newLogSegment(r *vdr.LogSegment, towrite bool) (*logSegment, error) {
	prefer, err := newSegmentBlockExt(r, towrite)
	if err != nil {
		return nil, err
	}

	segment := &logSegment{
		id:          r.ID,
		startOffset: r.StartOffset,
		endOffset:   atomic.Int64{},
		writable:    atomic.Bool{},
		prefer:      prefer,
	}
	if !r.Writable {
		segment.endOffset.Store(r.EndOffset)
	} else {
		segment.endOffset.Store(math.MaxInt64)
		segment.writable.Store(true)
	}
	return segment, nil
}

func newSegmentBlockExt(r *vdr.LogSegment, leaderOnly bool) (*segmentBlock, error) {
	id := r.LeaderBlockID
	if id == 0 {
		if leaderOnly {
			return nil, errors.ErrNoLeader
		}
		for _, b := range r.Blocks {
			if b.Endpoint != "" {
				id = b.ID
				break
			}
		}
	}
	b, ok := r.Blocks[id]
	if !ok {
		return nil, errors.ErrNoBlock
	}
	return newSegmentBlock(b)
}

type logSegment struct {
	id          uint64
	startOffset int64
	endOffset   atomic.Int64
	writable    atomic.Bool

	prefer *segmentBlock
	mu     sync.RWMutex
}

func (s *logSegment) ID() uint64 {
	return s.id
}

func (s *logSegment) StartOffset() int64 {
	return s.startOffset
}

func (s *logSegment) EndOffset() int64 {
	return s.endOffset.Load()
}

func (s *logSegment) Writable() bool {
	return s.writable.Load()
}

func (s *logSegment) SetNotWritable() {
	s.writable.Store(false)
}

func (s *logSegment) Close() {
	s.prefer.Close()
}

func (s *logSegment) Update(r *vdr.LogSegment, towrite bool) error {
	// When a segment become read-only, the end offset needs to be set to the readlly value.
	if s.Writable() && !r.Writable && s.writable.CAS(true, false) {
		s.endOffset.Store(r.EndOffset)
		return nil
	}

	switchBlock := func() bool {
		if towrite {
			if s.prefer.id != r.LeaderBlockID {
				return true
			}
		} else {
			if _, ok := r.Blocks[s.prefer.id]; !ok {
				return true
			}
		}
		return false
	}()
	if switchBlock {
		prefer, err := newSegmentBlockExt(r, true)
		if err != nil {
			return err
		}
		s.setPreferSegmentBlock(prefer)
	}

	return nil
}

func (s *logSegment) Append(ctx context.Context, event *ce.Event) (int64, error) {
	b := s.preferSegmentBlock()
	if b == nil {
		return -1, errors.ErrNoLeader
	}
	off, err := b.Append(ctx, event)
	if err != nil {
		return -1, err
	}
	return off + s.startOffset, nil
}

func (s *logSegment) Read(ctx context.Context, from int64, size int16) ([]*ce.Event, error) {
	if from < s.startOffset {
		return nil, errors.ErrUnderflow
	}
	if eo := s.endOffset.Load(); eo >= 0 {
		if from > eo {
			return nil, errors.ErrOverflow
		}
		if int64(size) > eo-from {
			size = int16(eo - from)
		}
	}
	// TODO: cached read
	b := s.preferSegmentBlock()
	if b == nil {
		return nil, errors.ErrNoBlock
	}
	events, err := b.Read(ctx, from-s.startOffset, size)
	if err != nil {
		return nil, err
	}

	for _, e := range events {
		eventBlockOff, _ := e.Extensions()["xvanusblockoff"].(int32)
		offset := s.startOffset + int64(eventBlockOff)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(offset))
		e.SetExtension("xvanuslogoff", buf)
	}

	return events, err
}

func (s *logSegment) preferSegmentBlock() *segmentBlock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prefer
}

func (s *logSegment) setPreferSegmentBlock(prefer *segmentBlock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefer = prefer
}
