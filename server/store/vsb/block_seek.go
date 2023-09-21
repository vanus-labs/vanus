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

package vsb

import (
	// standard libraries.
	"context"
	"sort"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/vanus-labs/vanus/server/store/block"
	ceschema "github.com/vanus-labs/vanus/server/store/schema/ce"
	"github.com/vanus-labs/vanus/server/store/vsb/index"
)

// Make sure block implements block.Reader.
var _ block.Seeker = (*vsBlock)(nil)

func (b *vsBlock) Seek(ctx context.Context, index int64, key block.Entry, flag block.SeekKeyFlag) (int64, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.vsb.vsBlock.Seek() Start")
	defer span.AddEvent("store.vsb.vsBlock.Seek() End")

	b.mu.RLock()
	indexes := b.indexes
	b.mu.RUnlock()

	switch flag {
	case block.SeekKeyExact:
		return b.seekKeyExact(ctx, index, key, indexes)
	case block.SeekKeyOrNext:
		return b.seekKeyOrNext(ctx, index, key, indexes)
	case block.SeekKeyOrPrev:
		return b.seekKeyOrPrev(ctx, index, key, indexes)
	case block.SeekAfterKey:
		return b.seekAfterKey(ctx, index, key, indexes)
	case block.SeekBeforeKey:
		return b.seekBeforeKey(ctx, index, key, indexes)
	default:
		return -1, block.ErrNotSupported
	}
}

func (b *vsBlock) seekKeyExact(_ context.Context, idx int64, key block.Entry, indexes []index.Index) (int64, error) {
	cmp := b.selectComparer(idx, key)
	seq := searchGE(indexes, cmp)
	if seq >= 0 && cmp(indexes[seq]) == 0 {
		return seq, nil
	}
	return -1, nil
}

func (b *vsBlock) seekKeyOrNext(_ context.Context, idx int64, key block.Entry, indexes []index.Index) (int64, error) {
	return searchGE(indexes, b.selectComparer(idx, key)), nil
}

func (b *vsBlock) seekKeyOrPrev(_ context.Context, idx int64, key block.Entry, indexes []index.Index) (int64, error) {
	cmp := b.selectComparer(idx, key)
	seq := searchGE(indexes, cmp)
	if seq >= 0 && cmp(indexes[seq]) != 0 {
		return seq - 1, nil
	}
	return seq, nil
}

func (b *vsBlock) seekAfterKey(_ context.Context, idx int64, key block.Entry, indexes []index.Index) (int64, error) {
	return searchGT(indexes, b.selectComparer(idx, key)), nil
}

func (b *vsBlock) seekBeforeKey(_ context.Context, idx int64, key block.Entry, indexes []index.Index) (int64, error) {
	seq := searchGE(indexes, b.selectComparer(idx, key))
	if seq >= 0 {
		return seq - 1, nil
	}
	return int64(len(indexes)) - 1, nil
}

func (b *vsBlock) selectComparer(_ int64, key block.Entry) func(index.Index) int {
	// TODO(james.yin): support non-stime index.
	val := ceschema.Stime(key)
	return func(i index.Index) int {
		switch v := i.Stime(); {
		case v == val:
			return 0
		case v > val:
			return 1
		default: // v > val
			return -1
		}
	}
}

func searchGE(indexes []index.Index, cmp func(index.Index) int) int64 {
	sz := len(indexes)
	seq := sort.Search(sz, func(i int) bool {
		return cmp(indexes[i]) >= 0
	})
	if seq < sz {
		return int64(seq)
	}
	return -1
}

func searchGT(indexes []index.Index, cmp func(index.Index) int) int64 {
	sz := len(indexes)
	seq := sort.Search(sz, func(i int) bool {
		return cmp(indexes[i]) > 0
	})
	if seq < sz {
		return int64(seq)
	}
	return -1
}
