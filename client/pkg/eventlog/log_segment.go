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
	// standard libraries.
	"context"
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/atomic"

	// first-party libraries.
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.

	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/record"
)

func newSegment(ctx context.Context, r *record.Segment, towrite bool) (*segment, error) {
	prefer, err := newBlockExt(ctx, r, towrite)
	if err != nil {
		return nil, err
	}

	segment := &segment{
		id:               r.ID,
		startOffset:      r.StartOffset,
		endOffset:        atomic.Int64{},
		writable:         atomic.Bool{},
		firstEventBornAt: r.FirstEventBornAt,
		lastEventBornAt:  r.LastEventBornAt,
		prefer:           prefer,
		tracer:           tracing.NewTracer("internal.eventlog.segment", trace.SpanKindClient),
	}

	if !r.Writable {
		segment.endOffset.Store(r.EndOffset)
	} else {
		segment.endOffset.Store(math.MaxInt64)
		segment.writable.Store(true)
	}
	return segment, nil
}

func newBlockExt(ctx context.Context, r *record.Segment, leaderOnly bool) (*block, error) {
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
	return newBlock(ctx, b)
}

type segment struct {
	id               uint64
	startOffset      int64
	endOffset        atomic.Int64
	writable         atomic.Bool
	firstEventBornAt time.Time
	lastEventBornAt  time.Time

	prefer *block
	mu     sync.RWMutex
	tracer *tracing.Tracer
}

func (s *segment) ID() uint64 {
	return s.id
}

func (s *segment) StartOffset() int64 {
	return s.startOffset
}

func (s *segment) EndOffset() int64 {
	return s.endOffset.Load()
}

func (s *segment) Writable() bool {
	return s.writable.Load()
}

func (s *segment) SetNotWritable() {
	s.writable.Store(false)
}

func (s *segment) Close(ctx context.Context) {
	s.prefer.Close(ctx)
}

func (s *segment) Update(ctx context.Context, r *record.Segment, towrite bool) error {
	// When a segment become read-only, the end offset needs to be set to the real value.
	// TODO(wenfeng) data race?
	s.lastEventBornAt = r.LastEventBornAt
	if s.Writable() && !r.Writable && s.writable.CAS(true, false) {
		s.endOffset.Store(r.EndOffset)
		return nil
	}

	_, span := s.tracer.Start(ctx, "Update")
	defer span.End()

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
		prefer, err := newBlockExt(ctx, r, true)
		if err != nil {
			return err
		}
		s.setPreferSegmentBlock(prefer)
	}

	return nil
}

func (s *segment) Append(ctx context.Context, event *ce.Event) (int64, error) {
	_ctx, span := s.tracer.Start(ctx, "Append")
	defer span.End()

	b := s.preferSegmentBlock()
	if b == nil {
		return -1, errors.ErrNoLeader
	}
	off, err := b.Append(_ctx, event)
	if err != nil {
		return -1, err
	}
	return off + s.startOffset, nil
}

func (s *segment) Read(ctx context.Context, from int64, size int16, pollingTimeout uint32) ([]*ce.Event, error) {
	if from < s.startOffset {
		return nil, errors.ErrUnderflow
	}
	ctx, span := s.tracer.Start(ctx, "Read")
	defer span.End()

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
	events, err := b.Read(ctx, from-s.startOffset, size, pollingTimeout)
	if err != nil {
		return nil, err
	}

	for _, e := range events {
		v, ok := e.Extensions()[segpb.XVanusBlockOffset]
		if !ok {
			continue
		}
		off, ok := v.(int32)
		if !ok {
			return events, errors.ErrCorruptedEvent
		}
		offset := s.startOffset + int64(off)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(offset))
		e.SetExtension(XVanusLogOffset, buf)
		e.SetExtension(segpb.XVanusBlockOffset, nil)
	}

	return events, err
}

func (s *segment) preferSegmentBlock() *block {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prefer
}

func (s *segment) setPreferSegmentBlock(prefer *block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefer = prefer
}

func (s *segment) LookupOffset(ctx context.Context, t time.Time) (int64, error) {
	return s.preferSegmentBlock().LookupOffset(ctx, t)
}
