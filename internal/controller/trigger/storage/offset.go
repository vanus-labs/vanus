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
	"encoding/json"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"path"
)

type OffsetStorage interface {
	CreateOffset(ctx context.Context, subId string, info info.ListOffsetInfo) error
	UpdateOffset(ctx context.Context, subId string, info info.ListOffsetInfo) error
	GetOffset(ctx context.Context, subId string) (info.ListOffsetInfo, error)
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

func (s *offsetStorage) getKey(subId string) string {
	return path.Join(primitive.StorageOffset.String(), subId)
}

func (s *offsetStorage) CreateOffset(ctx context.Context, subId string, info info.ListOffsetInfo) error {
	v, err := json.Marshal(info)
	if err != nil {
		return errors.ErrJsonMarshal.Wrap(err)
	}
	return s.client.Create(ctx, s.getKey(subId), v)
}

func (s *offsetStorage) UpdateOffset(ctx context.Context, subId string, info info.ListOffsetInfo) error {
	v, err := json.Marshal(info)
	if err != nil {
		return errors.ErrJsonMarshal.Wrap(err)
	}
	return s.client.Update(ctx, s.getKey(subId), v)
}

func (s *offsetStorage) GetOffset(ctx context.Context, subId string) (info.ListOffsetInfo, error) {
	v, err := s.client.Get(ctx, s.getKey(subId))
	if err != nil {
		return nil, err
	}
	var infos []info.OffsetInfo
	err = json.Unmarshal(v, &infos)
	if err != nil {
		return nil, errors.ErrJsonUnMarshal.Wrap(err)
	}

	return infos, nil
}

func (s *offsetStorage) DeleteOffset(ctx context.Context, subId string) error {
	return s.client.Delete(ctx, s.getKey(subId))
}
