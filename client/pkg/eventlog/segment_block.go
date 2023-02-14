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
	"time"

	// third-party libraries

	// first-party libraries
	"github.com/linkall-labs/vanus/pkg/errors"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/store"
	"github.com/linkall-labs/vanus/client/pkg/record"
)

func newBlock(ctx context.Context, r *record.Block) (*block, error) {
	store, err := store.Get(ctx, r.Endpoint)
	if err != nil {
		return nil, err
	}
	block := block{
		id:    r.ID,
		store: store,
	}
	return &block, nil
}

type block struct {
	id    uint64
	store *store.BlockStore
}

func (s *block) Close(ctx context.Context) {
	store.Put(ctx, s.store)
}

func (s *block) LookupOffset(ctx context.Context, t time.Time) (int64, error) {
	return s.store.LookupOffset(ctx, s.id, t)
}

func (s *block) Append(ctx context.Context, event *cloudevents.CloudEventBatch) ([]int64, error) {
	return s.store.Append(ctx, s.id, event)
}

func (s *block) Read(ctx context.Context, offset int64, size int16, pollingTimeout uint32) (*cloudevents.CloudEventBatch, error) {
	if offset < 0 {
		return nil, errors.ErrOffsetUnderflow
	}
	if size > 0 {
		// doRead
	} else if size == 0 {
		return &cloudevents.CloudEventBatch{
			Events: []*cloudevents.CloudEvent{},
		}, nil
	} else if size < 0 {
		return nil, errors.ErrInvalidArgument
	}
	return s.store.Read(ctx, s.id, offset, size, pollingTimeout)
}
