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

//go:generate mockgen -source=offset.go -destination=mock_offset.go -package=storage
package storage

import (
	"context"
	"path"
	"path/filepath"
	"strconv"

	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

type OffsetStorage interface {
	CreateOffset(ctx context.Context, subscriptionID vanus.ID, info info.OffsetInfo) error
	UpdateOffset(ctx context.Context, subscriptionID vanus.ID, info info.OffsetInfo) error
	GetOffsets(ctx context.Context, subscriptionID vanus.ID) (info.ListOffsetInfo, error)
	DeleteOffset(ctx context.Context, subscriptionID vanus.ID) error
}

var (
	base    = 10
	bitSize = 64
)

type offsetStorage struct {
	client kv.Client
}

func NewOffsetStorage(client kv.Client) OffsetStorage {
	return &offsetStorage{
		client: client,
	}
}

func (s *offsetStorage) getKey(subscriptionID, eventlogID vanus.ID) string {
	return path.Join(kv.MetadataOffset, subscriptionID.Key(), eventlogID.Key())
}

func (s *offsetStorage) getSubKey(subscriptionID vanus.ID) string {
	return path.Join(kv.MetadataSecret, subscriptionID.Key())
}

func (s *offsetStorage) int64ToByteArr(v uint64) []byte {
	/*
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, vanus.VolumeID(v))
		return b
	*/
	str := strconv.FormatUint(v, base)
	return []byte(str)
}

func (s *offsetStorage) byteArrToUint64(b []byte) uint64 {
	/*
		v := binary.LittleEndian.vanus.VolumeID(b)
		return int64(v)
	*/
	v, _ := strconv.ParseUint(string(b), base, bitSize)
	return v
}

func (s *offsetStorage) CreateOffset(ctx context.Context, subscriptionID vanus.ID, info info.OffsetInfo) error {
	return s.client.Create(ctx, s.getKey(subscriptionID, info.EventlogID), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) UpdateOffset(ctx context.Context, subscriptionID vanus.ID, info info.OffsetInfo) error {
	return s.client.Update(ctx, s.getKey(subscriptionID, info.EventlogID), s.int64ToByteArr(info.Offset))
}

func (s *offsetStorage) GetOffsets(ctx context.Context, subscriptionID vanus.ID) (info.ListOffsetInfo, error) {
	l, err := s.client.List(ctx, s.getSubKey(subscriptionID))
	if err != nil {
		return nil, err
	}
	var infos info.ListOffsetInfo
	for _, v := range l {
		id, err := vanus.NewIDFromString(filepath.Base(v.Key))
		if err != nil {
			return nil, err
		}
		infos = append(infos, info.OffsetInfo{EventlogID: id, Offset: s.byteArrToUint64(v.Value)})
	}
	return infos, nil
}

func (s *offsetStorage) DeleteOffset(ctx context.Context, subscriptionID vanus.ID) error {
	return s.client.DeleteDir(ctx, s.getSubKey(subscriptionID))
}
