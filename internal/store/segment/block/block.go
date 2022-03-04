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

package block

import (
	"context"
	"errors"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"os"
	"path/filepath"
)

var (
	ErrNoEnoughCapacity = errors.New("no enough capacity")
	ErrOffsetExceeded   = errors.New("the offset exceeded")
)

type SegmentBlockWriter interface {
	Append(context.Context, ...*codec.StoredEntry) error
	CloseWrite(context.Context) error
	IsAppendable() bool
}

type SegmentBlockReader interface {
	Read(context.Context, int, int) ([]*codec.StoredEntry, error)
	CloseRead(context.Context) error
	IsReadable() bool
}

type SegmentBlock interface {
	SegmentBlockWriter
	SegmentBlockReader

	Path() string
	IsFull() bool
	IsEmpty() bool
	SegmentBlockID() string
	Close(context.Context) error
	Initialize(context.Context) error
	HealthInfo() *meta.SegmentHealthInfo
}

func CreateFileSegmentBlock(ctx context.Context, id string, path string, capacity int64) (SegmentBlock, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	b := &fileBlock{
		id:          id,
		path:        path,
		capacity:    capacity,
		writeOffset: fileSegmentBlockHeaderCapacity,
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err = f.Truncate(capacity); err != nil {
		return nil, err
	}
	if _, err = f.Seek(fileSegmentBlockHeaderCapacity, 0); err != nil {
		return nil, err
	}
	b.appendable.Store(true)
	b.readable.Store(true)
	b.fullFlag.Store(false)
	b.physicalFile = f
	if err = b.persistHeader(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func OpenFileSegmentBlock(ctx context.Context, path string) (SegmentBlock, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	b := &fileBlock{
		id:   filepath.Base(path),
		path: path,
	}
	b.appendable.Store(true)
	b.readable.Store(true)
	b.fullFlag.Store(false)
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	b.physicalFile = f
	return b, nil
}
