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
	"fmt"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/pkg/errors"
	"path"
	"strconv"
)

type OffsetStorage interface {
	CreateOffset(ctx context.Context, info *info.OffsetInfo) error
	UpdateOffset(ctx context.Context, info *info.OffsetInfo) error
	GetOffset(ctx context.Context, info *info.OffsetInfo) (int64, error)
	ListOffset(ctx context.Context, subId string) ([]*info.OffsetInfo, error)
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

func (s *offsetStorage) Close() error {
	return s.client.Close()
}

func (s *offsetStorage) CreateOffset(ctx context.Context, info *info.OffsetInfo) error {
	key := path.Join(primitive.StorageOffset.String(), info.SubId, info.EventLog)
	v := []byte(fmt.Sprintf("%d", info.Offset))
	return s.client.Create(ctx, key, v)
}

func (s *offsetStorage) UpdateOffset(ctx context.Context, info *info.OffsetInfo) error {
	key := path.Join(primitive.StorageOffset.String(), info.SubId, info.EventLog)
	return s.client.Update(ctx, key, []byte(fmt.Sprintf("%d", info.Offset)))
}

func (s *offsetStorage) GetOffset(ctx context.Context, info *info.OffsetInfo) (int64, error) {
	key := path.Join(primitive.StorageOffset.String(), info.SubId, info.EventLog)
	v, err := s.client.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	intV, err := strconv.ParseInt(string(v), 10, 0)
	if err != nil {
		return 0, errors.Wrapf(err, "%s parseInt error", string(v))
	}
	return intV, nil
}

func (s *offsetStorage) ListOffset(ctx context.Context, subId string) ([]*info.OffsetInfo, error) {
	key := path.Join(primitive.StorageOffset.String(), subId, "/")
	l, err := s.client.List(ctx, key)
	if err != nil {
		return nil, err
	}
	var list []*info.OffsetInfo
	for _, p := range l {
		intV, err := strconv.ParseInt(string(p.Value), 10, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "%s parseInt error", string(p.Value))
		}
		list = append(list, &info.OffsetInfo{
			SubId:    subId,
			EventLog: p.Key,
			Offset:   intV,
		})
	}
	return list, nil
}

func (s *offsetStorage) DeleteOffset(ctx context.Context, subId string) error {
	key := path.Join(primitive.StorageOffset.String(), subId)
	return s.client.DeleteDir(ctx, key)
}
