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
	"path"
	"path/filepath"
	"strconv"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type OffsetStorage interface {
	CreateOffset(ctx context.Context, subId vanus.ID, info info.OffsetInfo) error
	UpdateOffset(ctx context.Context, subId vanus.ID, info info.OffsetInfo) error
	GetOffsets(ctx context.Context, subId vanus.ID) (info.ListOffsetInfo, error)
	DeleteOffset(ctx context.Context, subId vanus.ID) error
}

type offsetStorage struct {
	client kv.Client
}

func NewOffsetStorage(client kv.Client) OffsetStorage {
	return &offsetStorage{
		client: client,
	}
}

func (s *offsetStorage) getKey(subID, eventLogID vanus.ID) string {
	return path.Join(StorageOffset.String(), subID.String(), eventLogID.String())
}

func (s *offsetStorage) getSubKey(subID vanus.ID) string {
	return path.Join(StorageOffset.String(), subID.String())
}

func (s *offsetStorage) int64ToByteArr(v uint64) []byte {
	//b := make([]byte, 8)
	//binary.LittleEndian.PutUint64(b, vanus.ID(v))
	//return b
	str := strconv.FormatUint(v, 10)
	return []byte(str)
}

func (s *offsetStorage) byteArrToUint64(b []byte) uint64 {
	//v := binary.LittleEndian.vanus.ID(b)
	//return int64(v)
	v, _ := strconv.ParseUint(string(b), 10, 64)
	return v
}

func (s *offsetStorage) CreateOffset(ctx context.Context, subId vanus.ID, info info.OffsetInfo) error {
	return s.client.Create(ctx, s.getKey(subId, info.EventLogID), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) UpdateOffset(ctx context.Context, subId vanus.ID, info info.OffsetInfo) error {
	return s.client.Update(ctx, s.getKey(subId, info.EventLogID), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) GetOffsets(ctx context.Context, subId vanus.ID) (info.ListOffsetInfo, error) {
	l, err := s.client.List(ctx, s.getSubKey(subId))
	if err != nil {
		return nil, err
	}
	var infos info.ListOffsetInfo
	for _, v := range l {
		id, _ := vanus.StringToID(filepath.Base(v.Key))
		infos = append(infos, info.OffsetInfo{EventLogID: id, Offset: s.byteArrToUint64(v.Value)})
	}
	return infos, nil
}

func (s *offsetStorage) DeleteOffset(ctx context.Context, subId vanus.ID) error {
	return s.client.DeleteDir(ctx, s.getSubKey(subId))
}
