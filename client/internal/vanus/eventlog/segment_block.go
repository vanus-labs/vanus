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
	// third-party libraries
	"context"
	ce "github.com/cloudevents/sdk-go/v2"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/discovery/record"
	"github.com/linkall-labs/vanus/client/internal/vanus/store"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

func newSegmentBlock(ctx context.Context, r *record.SegmentBlock) (*segmentBlock, error) {
	store, err := store.Get(ctx, r.Endpoint)
	if err != nil {
		return nil, err
	}
	block := segmentBlock{
		id:    r.ID,
		store: store,
	}
	return &block, nil
}

type segmentBlock struct {
	id    uint64
	store *store.BlockStore
}

func (s *segmentBlock) Close(ctx context.Context) {
	store.Put(ctx, s.store)
}

func (s *segmentBlock) Append(ctx context.Context, event *ce.Event) (int64, error) {
	return s.store.Append(ctx, s.id, event)
}

func (s *segmentBlock) Read(ctx context.Context, offset int64, size int16, pollingTimeout uint32) ([]*ce.Event, error) {
	if offset < 0 {
		return nil, errors.ErrUnderflow
	}
	if size > 0 {
		// doRead
	} else if size == 0 {
		return make([]*ce.Event, 0, 0), nil
	} else if size < 0 {
		return nil, errors.ErrInvalidArgument
	}
	return s.store.Read(ctx, s.id, offset, size, pollingTimeout)
}
