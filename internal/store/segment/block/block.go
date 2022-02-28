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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"io"
	"os"
	"sync/atomic"
)

var (
	ErrNoEnoughCapacity = errors.New("no enough capacity")
	ErrOffsetExceeded   = errors.New("the offset exceeded")
)

const (
	segmentHeaderSize = 4096
)

type StorageBlockWriter interface {
	Append(context.Context, ...*codec.StoredEntry) error
	CloseWrite(context.Context) error
}

type StorageBlockReader interface {
	Read(context.Context, int, int) ([]*codec.StoredEntry, error)
	CloseRead(context.Context) error
}

type StorageBlockReadWriter interface {
	StorageBlockReader
	StorageBlockWriter
}

func CreateSegmentBlock(ctx context.Context, id string, path string, capacity int64) (StorageBlockReadWriter, error) {
	b := &block{
		id:          id,
		path:        path,
		capacity:    capacity,
		writeOffset: segmentHeaderSize,
		readOffset:  segmentHeaderSize,
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err = f.Truncate(capacity); err != nil {
		return nil, err
	}
	if _, err = f.Seek(segmentHeaderSize, 0); err != nil {
		return nil, err
	}
	b.physicalFile = f
	return b, nil
}

func OpenSegmentBlock(ctx context.Context, path string) (StorageBlockReader, error) {
	b := &block{
		readOffset: segmentHeaderSize,
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	b.physicalFile = f
	if err := b.loadHeader(); err != nil {
		return nil, err
	}
	if err := b.validate(); err != nil {
		return nil, err
	}
	if err := b.loadIndex(); err != nil {
		return nil, err
	}
	return b, nil
}

type block struct {
	version      int32
	id           string
	path         string
	capacity     int64
	length       int64
	number       int32
	writeOffset  int64
	readOffset   int64
	physicalFile *os.File
	indexes      []blockIndex
}

func (b *block) Append(ctx context.Context, entities ...*codec.StoredEntry) error {
	if len(entities) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(make([]byte, 0))
	length := 0
	idxes := make([]blockIndex, 0)
	for idx := range entities {
		data, err := codec.Marshall(entities[idx])
		if err != nil {
			return err
		}
		bi := blockIndex{
			startOffset: b.writeOffset + int64(length),
		}
		if _, err = buf.Write(data); err != nil {
			return err
		}
		length += len(data)
		bi.length = int32(len(data))
		idxes = append(idxes, bi)
	}
	// TODO optimize this
	// if the file has been left many space, but received a large request, the remain space will be wasted
	if length > b.remain(int64(length+12*len(idxes))) {
		return ErrNoEnoughCapacity
	}
	n, err := b.physicalFile.Write(buf.Bytes())
	b.indexes = append(b.indexes, idxes...)
	atomic.AddInt32(&b.number, int32(len(idxes)))
	atomic.AddInt64(&b.writeOffset, int64(n))
	return err
}

func (b *block) Read(ctx context.Context, entityStartOffset, number int) ([]*codec.StoredEntry, error) {
	from, to, err := b.calculateRange(entityStartOffset, number)
	if err != nil {
		return nil, err
	}

	data := make([]byte, to-from)
	if _, err := b.physicalFile.ReadAt(data, from); err != nil {
		return nil, err
	}

	ses := make([]*codec.StoredEntry, 0)
	reader := bytes.NewReader(data)
	for err == nil {
		size := int32(0)
		if err = binary.Read(reader, binary.BigEndian, &size); err != nil {
			break
		}
		payload := make([]byte, int(size))
		if _, err = reader.Read(payload); err != nil {
			break
		}
		se := &codec.StoredEntry{
			Length:  size,
			Payload: payload,
		}
		ses = append(ses, se)
	}
	if err != io.EOF {
		return nil, err
	}

	return ses, nil
}

func (b *block) CloseWrite(ctx context.Context) error {
	if err := b.physicalFile.Close(); err != nil {
		return err
	}
	return nil
}

func (b *block) CloseRead(ctx context.Context) error {
	if err := b.physicalFile.Close(); err != nil {
		return err
	}
	return nil
}

func (b *block) remain(sizeNeedServed int64) int {
	return int(b.capacity-b.length-int64(b.number*12)-sizeNeedServed) - segmentHeaderSize
}

func (b *block) loadHeader() error {
	return nil
}

func (b *block) validate() error {
	return nil
}

func (b *block) loadIndex() error {
	return nil
}

func (b *block) calculateRange(start, num int) (int64, int64, error) {
	if start >= len(b.indexes) {
		return -1, -1, ErrOffsetExceeded
	}
	startPos := b.indexes[start].startOffset
	var endPos int64
	offset := start + num - 1
	if b.number < int32(offset+1) {
		endPos = b.indexes[b.number-1].startOffset + int64(b.indexes[b.number-1].length)
	} else {
		endPos = b.indexes[offset].startOffset + int64(b.indexes[offset].length)
	}
	return startPos, endPos, nil
}

type blockIndex struct {
	startOffset int64
	length      int32
}
