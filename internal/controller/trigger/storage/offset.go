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

package storage

import (
	"context"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"path"
	"path/filepath"
	"strconv"
)

type OffsetStorage interface {
	CreateOffset(ctx context.Context, subId string, info info.OffsetInfo) error
	UpdateOffset(ctx context.Context, subId string, info info.OffsetInfo) error
	GetOffsets(ctx context.Context, subId string) (info.ListOffsetInfo, error)
	DeleteOffset(ctx context.Context, subId string) error
}

type offsetStorage struct {
	client kv.Client
}

func NewOffsetStorage(client kv.Client) OffsetStorage {
	return &offsetStorage{
		client: client,
	}
}

func (s *offsetStorage) getKey(subId, eventLog string) string {
	return path.Join(primitive.StorageOffset.String(), subId, eventLog)
}

func (s *offsetStorage) getSubKey(subId string) string {
	return path.Join(primitive.StorageOffset.String(), subId)
}

func (s *offsetStorage) int64ToByteArr(v int64) []byte {
	//b := make([]byte, 8)
	//binary.LittleEndian.PutUint64(b, uint64(v))
	//return b
	str := strconv.FormatInt(v, 10)
	return []byte(str)
}

func (s *offsetStorage) byteArr2Int64(b []byte) int64 {
	//v := binary.LittleEndian.Uint64(b)
	//return int64(v)
	v, _ := strconv.ParseInt(string(b), 10, 64)
	return v
}

func (s *offsetStorage) CreateOffset(ctx context.Context, subId string, info info.OffsetInfo) error {
	return s.client.Create(ctx, s.getKey(subId, info.EventLogId), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) UpdateOffset(ctx context.Context, subId string, info info.OffsetInfo) error {
	return s.client.Update(ctx, s.getKey(subId, info.EventLogId), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) GetOffsets(ctx context.Context, subId string) (info.ListOffsetInfo, error) {
	l, err := s.client.List(ctx, s.getSubKey(subId))
	if err != nil {
		return nil, err
	}
	var infos info.ListOffsetInfo
	for _, v := range l {
		infos = append(infos, info.OffsetInfo{EventLogId: filepath.Base(v.Key), Offset: s.byteArr2Int64(v.Value)})
	}
	return infos, nil
}

func (s *offsetStorage) DeleteOffset(ctx context.Context, subId string) error {
	return s.client.DeleteDir(ctx, s.getSubKey(subId))
}
