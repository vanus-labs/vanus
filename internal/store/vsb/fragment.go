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
	"encoding/binary"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/vsb/codec"
)

const (
	OffsetSize = 8

	PayloadOffset = OffsetSize
)

type fragment struct {
	offset  int64
	entries []block.Entry
	enc     codec.EntryEncoder
	data    []byte
}

// Make sure fragment implements block.Fragment and block.FragmentMarshaler.
var (
	_ block.Fragment          = (*fragment)(nil)
	_ block.FragmentMarshaler = (*fragment)(nil)
)

func newFragment(offset int64, entries []block.Entry, enc codec.EntryEncoder) block.Fragment {
	return &fragment{
		offset:  offset,
		entries: entries,
		enc:     enc,
	}
}

func (f *fragment) Payload() []byte {
	data, err := f.MarshalFragment(context.Background())
	if err != nil {
		panic(err)
	}
	return data[PayloadOffset:]
}

func (f *fragment) Size() int {
	if f.data != nil {
		return len(f.data) - OffsetSize
	}
	return f.size()
}

func (f *fragment) StartOffset() int64 {
	return f.offset
}

func (f *fragment) EndOffset() int64 {
	return f.StartOffset() + int64(f.Size())
}

func (f *fragment) MarshalFragment(ctx context.Context) ([]byte, error) {
	if f.data != nil {
		return f.data, nil
	}

	data, err := f.doMarshal(ctx)
	if err != nil {
		return nil, err
	}
	f.data = data
	return data, nil
}

func (f *fragment) size() int {
	var sz int
	for _, entry := range f.entries {
		sz += f.enc.Size(entry)
	}
	return sz
}

func (f *fragment) doMarshal(ctx context.Context) ([]byte, error) {
	data := make([]byte, OffsetSize+f.size())

	binary.LittleEndian.PutUint64(data, uint64(f.offset))

	off := PayloadOffset
	for _, entry := range f.entries {
		n, _ := f.enc.MarshalTo(ctx, entry, data[off:])
		off += n
	}

	return data, nil
}
