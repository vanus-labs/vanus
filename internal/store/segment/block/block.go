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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"io"
	"os"
)

var (
	ErrNoEnoughCapacity = errors.New("no enough capacity")
)

type StorageBlockWriter interface {
	Append(context.Context, ...*codec.StoredEntry) error
	CloseWrite(context.Context) error
}

type StorageBlockReader interface {
	Read(context.Context, int, int) ([]*codec.StoredEntry, error)
	CloseRead(context.Context) error
}

func CreateSegmentBlock(ctx context.Context, id string, path string, capacity int64) (StorageBlockWriter, error) {
	b := &block{
		id:       id,
		path:     path,
		capacity: capacity,
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err = f.Truncate(capacity); err != nil {
		return nil, err
	}
	b.physicalFile = f
	return b, nil
}

func OpenSegmentBlock(path string) (StorageBlockReader, error) {
	b := &block{}
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
	id           string
	path         string
	capacity     int64
	length       int64
	number       int32
	endOffset    int64
	readPosition int64
	physicalFile *os.File
}

func (b *block) Append(ctx context.Context, entities ...*codec.StoredEntry) error {
	if len(entities) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(make([]byte, 4))
	writer := bufio.NewWriter(buf)
	length := 0
	for idx := range entities {
		data, err := codec.Marshall(entities[idx])
		if err != nil {
			return err
		}
		if err = binary.Write(writer, binary.BigEndian, int32(len(data))); err != nil {
			return err
		}
		length += 4
		if _, err = writer.Write(data); err != nil {
			return err
		}
		length += len(data)
	}
	// TODO optimize dynamic slicing
	if length > b.remain() {
		return ErrNoEnoughCapacity
	}
	_, err := b.physicalFile.Write(buf.Bytes())
	return err
}

func (b *block) Read(ctx context.Context, entityStartOffset, number int) ([]*codec.StoredEntry, error) {
	from, to, err := b.calculateRange(entityStartOffset, number)
	if err != nil {
		return nil, err
	}
	if b.readPosition != from {
		if _, err := b.physicalFile.Seek(from, 0); err != nil {
			return nil, err
		}
	}
	data := make([]byte, to-from)
	if _, err := b.physicalFile.ReadAt(data, from); err != nil {
		return nil, err
	}

	ses := make([]*codec.StoredEntry, number)
	reader := bytes.NewReader(data)
	count := 0
	for err == nil {
		size := int32(0)
		if err = binary.Read(reader, binary.BigEndian, &size); err != nil {
			break
		}
		payload := make([]byte, int(size))
		if _, err = reader.Read(payload); err != nil {
			break
		}
		se := &codec.StoredEntry{}
		if err := codec.Unmarshall(data, se); err != nil {
			break
		}
		ses[count] = se
		count++
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

func (b *block) remain() int {
	return int(b.capacity - b.length - int64(b.number*12))
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
	return 0, 0, nil
}

type blockIndex struct {
	startOffset int64
	endOffset   int32
}
