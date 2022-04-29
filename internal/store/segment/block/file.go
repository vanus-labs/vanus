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
	// standard libraries.
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	// third-party libraries.
	"go.uber.org/atomic"

	// first-party libraries.
	"github.com/linkall-labs/vsproto/pkg/meta"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	blockExt            = ".block"
	fileBlockHeaderSize = 4 * 1024

	// version + capacity + size + number + full
	v1FileBlockHeaderLength = 4 + 8 + 4 + 8 + 1
	entryLengthSize         = 4
)

func resolvePath(blockDir string, id vanus.ID) string {
	return filepath.Join(blockDir, fmt.Sprintf("%020d%s", id.Uint64(), blockExt))
}

type FileBlock struct {
	version int32
	id      vanus.ID
	path    string

	cap  int64
	size atomic.Int64
	num  atomic.Int32
	wo   atomic.Int64

	mux sync.Mutex

	f *os.File

	cis     ClusterInfoSource
	indexes []index

	readable   atomic.Bool
	appendable atomic.Bool
	full       atomic.Bool

	uncompletedReadRequestCount   atomic.Int32
	uncompletedAppendRequestCount atomic.Int32
}

func (b *FileBlock) Initialize(ctx context.Context) error {
	if err := b.loadHeader(ctx); err != nil {
		return err
	}
	b.wo.Store(fileBlockHeaderSize + b.size.Load())

	if b.full.Load() {
		b.appendable.Store(false)
	} else {
		if _, err := b.f.Seek(b.wo.Load(), 0); err != nil {
			return err
		}
	}

	if err := b.loadIndex(ctx); err != nil {
		return err
	}

	if err := b.validate(ctx); err != nil {
		return err
	}

	return nil
}

func (b *FileBlock) Append(ctx context.Context, entities ...Entry) error {
	observability.EntryMark(ctx)
	// TODO: optimize lock.
	b.mux.Lock()
	b.uncompletedAppendRequestCount.Add(1)
	defer func() {
		observability.LeaveMark(ctx)
		b.mux.Unlock()
		b.uncompletedAppendRequestCount.Sub(1)
	}()

	if len(entities) == 0 {
		return nil
	}

	length := 0
	for _, entry := range entities {
		length += entry.Size()
	}

	if length+v1IndexLength*len(entities) > b.remaining() {
		b.full.Store(true)
		return ErrNoEnoughCapacity
	}

	var so, wo int64 = 0, b.wo.Load()
	buf := make([]byte, length)
	indexs := make([]index, 0, len(entities))
	for _, entry := range entities {
		n, _ := entry.MarshalTo(buf[so:])
		bi := index{
			offset: wo + so,
			length: int32(n),
		}
		indexs = append(indexs, bi)
		so += int64(n)
	}

	n, err := b.f.Write(buf)
	if err != nil {
		return err
	}

	b.indexes = append(b.indexes, indexs...)

	b.num.Add(int32(len(indexs)))
	b.wo.Add(int64(n))
	b.size.Add(int64(n))

	//if err = b.physicalFile.Sync(); err != nil {
	//	return err
	//}

	return nil
}

func (b *FileBlock) appendWithOffset(ctx context.Context, entries ...Entry) error {
	if len(entries) == 0 {
		return nil
	}

	num := uint32(b.num.Load())
	for i := 0; i < len(entries); i++ {
		switch entry := &entries[i]; {
		case entry.Index < num:
			log.Warning(ctx, "block: entry index less than block num, skip this entry.", map[string]interface{}{
				"index": entry.Index,
				"num":   num,
			})
			continue
		case entry.Index > num:
			log.Error(ctx, "block: entry index greater than block num.", map[string]interface{}{
				"index": entry.Index,
				"num":   num,
			})
			return errors.ErrInternal
		}
		if i != 0 {
			entries = entries[i:]
		}
		break
	}
	if len(entries) == 0 {
		return nil
	}

	wo := uint32(b.wo.Load())
	offset := entries[0].Offset
	if offset != wo {
		log.Error(ctx, "block: entry offset is not equal than block wo.", map[string]interface{}{
			"offset": offset,
			"wo":     wo,
		})
		return errors.ErrInternal
	}

	for i := 1; i < len(entries); i++ {
		entry := &entries[i]
		prev := &entries[i-1]
		if prev.Index+1 != entry.Index {
			log.Error(ctx, "block: entry index is discontinuous.", map[string]interface{}{
				"index": entry.Index,
				"prev":  prev.Index,
			})
			return errors.ErrInternal
		}
		if prev.Offset+uint32(prev.Size()) != entry.Offset {
			log.Error(ctx, "block: entry offset is discontinuous.", map[string]interface{}{
				"offset": entry.Offset,
				"prev":   prev.Offset,
			})
			return errors.ErrInternal
		}
	}

	last := &entries[len(entries)-1]
	length := int(last.Offset-offset) + last.Size()

	// Check free space.
	if length+v1IndexLength*len(entries) > b.remaining() {
		b.full.Store(true)
		return ErrNoEnoughCapacity
	}

	buf := make([]byte, length)
	indexs := make([]index, 0, len(entries))
	for _, entry := range entries {
		n, _ := entry.MarshalTo(buf[entry.Offset-offset:])
		indexs = append(indexs, index{
			offset: int64(entry.Offset),
			length: int32(n),
		})
	}

	n, err := b.f.WriteAt(buf, int64(offset))
	if err != nil {
		return err
	}

	b.indexes = append(b.indexes, indexs...)

	b.num.Add(int32(len(entries)))
	b.wo.Add(int64(n))
	b.size.Add(int64(n))

	//if err = b.physicalFile.Sync(); err != nil {
	//	return err
	//}

	return nil
}

// Read date from file.
func (b *FileBlock) Read(ctx context.Context, entityStartOffset, number int) ([]Entry, error) {
	observability.EntryMark(ctx)
	b.uncompletedReadRequestCount.Add(1)
	defer func() {
		observability.LeaveMark(ctx)
		b.uncompletedReadRequestCount.Sub(1)
	}()

	from, to, num, err := b.calculateRange(entityStartOffset, number)
	if err != nil {
		return nil, err
	}

	size := uint32(to - from)
	data := make([]byte, size)
	if _, err2 := b.f.ReadAt(data, from); err2 != nil {
		return nil, err2
	}

	entries := make([]Entry, num)
	so := uint32(0)
	from2 := uint32(from)
	for i := 0; i < num; i++ {
		length := binary.BigEndian.Uint32(data[so : so+4])
		eo := so + entryLengthSize + length
		if eo > size {
			// TODO
		}
		entries[i].Offset = from2 + so
		entries[i].Payload = data[so+entryLengthSize : eo]
		so = eo
	}

	return entries, nil
}

func (b *FileBlock) CloseWrite(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	b.appendable.Store(false)
	for b.uncompletedAppendRequestCount.Load() != 0 {
		time.Sleep(time.Millisecond)
	}

	if err := b.persistHeader(ctx); err != nil {
		return err
	}
	if b.IsFull() {
		if err := b.persistIndex(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (b *FileBlock) CloseRead(ctx context.Context) error {
	if err := b.f.Close(); err != nil {
		return err
	}
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	b.readable.Store(false)
	for b.uncompletedReadRequestCount.Load() != 0 {
		time.Sleep(time.Millisecond)
	}
	return nil
}

func (b *FileBlock) Close(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return b.f.Close()
}

func (b *FileBlock) IsAppendable() bool {
	return b.appendable.Load() && !b.IsFull()
}

func (b *FileBlock) IsReadable() bool {
	return b.appendable.Load() && !b.IsEmpty()
}

func (b *FileBlock) IsEmpty() bool {
	return b.size.Load() == fileBlockHeaderSize
}

func (b *FileBlock) IsFull() bool {
	return b.full.Load()
}

func (b *FileBlock) Path() string {
	return b.path
}

func (b *FileBlock) SegmentBlockID() vanus.ID {
	return b.id
}

func (b *FileBlock) HealthInfo() *meta.SegmentHealthInfo {
	info := &meta.SegmentHealthInfo{
		Id:                   b.id.Uint64(),
		Size:                 b.size.Load(),
		EventNumber:          b.num.Load(),
		SerializationVersion: b.version,
		IsFull:               b.IsFull(),
	}

	// Fill cluster information.
	if cis := b.cis; cis != nil {
		cis.FillClusterInfo(info)
	}

	return info
}

func (b *FileBlock) remaining() int {
	// capacity - headerCapacity - dataLength - indexDataLength - currentRequestDataLength
	return int(b.cap - fileBlockHeaderSize - b.size.Load() -
		int64(b.num.Load()*v1IndexLength))
}

func (b *FileBlock) persistHeader(ctx context.Context) error {
	observability.EntryMark(ctx)
	b.mux.Lock()
	defer func() {
		b.mux.Unlock()
		observability.LeaveMark(ctx)
	}()

	buf := make([]byte, v1FileBlockHeaderLength)
	binary.BigEndian.PutUint32(buf[0:4], uint32(b.version))
	binary.BigEndian.PutUint64(buf[4:12], uint64(b.cap))
	binary.BigEndian.PutUint64(buf[12:20], uint64(b.size.Load()))
	binary.BigEndian.PutUint32(buf[20:24], uint32(b.num.Load()))
	if b.full.Load() {
		buf[24] = 1
	}

	// TODO: does it safe when concurrent write and append?
	if _, err := b.f.WriteAt(buf, 0); err != nil {
		return err
	}

	return nil
}

func (b *FileBlock) loadHeader(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	buf := make([]byte, v1FileBlockHeaderLength)
	if _, err := b.f.ReadAt(buf, 0); err != nil {
		return err
	}

	b.version = int32(binary.BigEndian.Uint32(buf[0:4]))
	b.cap = int64(binary.BigEndian.Uint64(buf[4:12]))
	b.size.Store(int64(binary.BigEndian.Uint64(buf[12:20])))
	b.num.Store(int32(binary.BigEndian.Uint32(buf[20:24])))
	b.full.Store(buf[24] != 0)

	return nil
}

func (b *FileBlock) persistIndex(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !b.IsFull() {
		return nil
	}

	length := v1IndexLength * len(b.indexes)
	buf := make([]byte, length)
	for i, index := range b.indexes {
		off := length - (i+1)*v1IndexLength
		_, _ = index.MarshalTo(buf[off : off+v1IndexLength])
	}

	if _, err := b.f.WriteAt(buf, b.cap-int64(length)); err != nil {
		return err
	}
	return nil
}

func (b *FileBlock) loadIndex(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	// rebuild index
	if !b.IsFull() {
		return b.rebuildIndex()
	}

	// read index directly
	return b.loadIndexFromFile()
}

func (b *FileBlock) loadIndexFromFile() error {
	num := int(b.num.Load())
	length := num * v1IndexLength

	// Read index data from file.
	data := make([]byte, length)
	if _, err := b.f.ReadAt(data, b.cap-int64(length)); err != nil {
		return err
	}

	// Decode indexes.
	b.indexes = make([]index, num)
	for i := range b.indexes {
		off := length - (i+1)*v1IndexLength
		b.indexes[i], _ = unmashalIndex(data[off : off+v1IndexLength])
	}

	return nil
}

func (b *FileBlock) rebuildIndex() error {
	num := b.num.Load()
	b.indexes = make([]index, 0, num)

	num = 0
	buf := make([]byte, entryLengthSize)
	off := int64(fileBlockHeaderSize)
	for {
		if _, err := b.f.ReadAt(buf, off); err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(buf)
		if length == 0 {
			break
		}
		b.indexes = append(b.indexes, index{
			offset: off,
			length: int32(length),
		})
		off += int64(entryLengthSize + length)
		num++
	}

	// Reset meta data.
	b.size.Store(off - fileBlockHeaderSize)
	b.num.Store(num)
	b.wo.Store(off)

	return nil
}

func (b *FileBlock) validate(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	return nil
}

func (b *FileBlock) calculateRange(start, num int) (int64, int64, int, error) {
	indexes := b.indexes
	if start >= len(indexes) {
		if !b.IsFull() && start == len(indexes) {
			return -1, -1, 0, ErrOffsetOnEnd
		}
		return -1, -1, 0, ErrOffsetExceeded
	}

	so := indexes[start].offset
	end := start + num - 1
	if end >= len(indexes) {
		end = len(indexes) - 1
	}
	eo := indexes[end].offset + int64(indexes[end].length) + entryLengthSize
	return so, eo, end - start + 1, nil
}

func (b *FileBlock) SetClusterInfoSource(cis ClusterInfoSource) {
	b.cis = cis
}
