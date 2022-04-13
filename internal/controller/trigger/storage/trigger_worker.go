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
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"path"
	"path/filepath"
)

type TriggerWorkerStorage interface {
	SaveTriggerWorker(context.Context, info.TriggerWorkerInfo) error
	GetTriggerWorker(ctx context.Context, id string) (*info.TriggerWorkerInfo, error)
	DeleteTriggerWorker(ctx context.Context, id string) error
	ListTriggerWorker(ctx context.Context) ([]*info.TriggerWorkerInfo, error)
}

type triggerWorkerStorage struct {
	client kv.Client
}

func NewTriggerWorkerStorage(client kv.Client) TriggerWorkerStorage {
	return &triggerWorkerStorage{
		client: client,
	}
}

func (s *triggerWorkerStorage) getKey(id string) string {
	return path.Join(primitive.StorageTriggerWorker.String(), id)
}

func (s *triggerWorkerStorage) SaveTriggerWorker(ctx context.Context, info info.TriggerWorkerInfo) error {
	key := s.getKey(info.Id)
	exist, err := s.client.Exists(ctx, key)
	if err != nil {
		return err
	}
	v, err := json.Marshal(info)
	if err != nil {
		return errors.ErrJsonMarshal.Wrap(err)
	}
	if !exist {
		return s.client.Create(ctx, key, v)
	}
	return s.client.Update(ctx, s.getKey(info.Id), v)
}
func (s *triggerWorkerStorage) GetTriggerWorker(ctx context.Context, id string) (*info.TriggerWorkerInfo, error) {
	v, err := s.client.Get(ctx, s.getKey(id))
	if err != nil {
		return nil, err
	}
	var tWorker info.TriggerWorkerInfo
	err = json.Unmarshal(v, &tWorker)
	if err != nil {
		return nil, errors.ErrJsonUnMarshal.Wrap(err)
	}
	return &tWorker, nil
}

func (s *triggerWorkerStorage) DeleteTriggerWorker(ctx context.Context, id string) error {
	return s.client.Delete(ctx, s.getKey(id))
}

func (s *triggerWorkerStorage) ListTriggerWorker(ctx context.Context) ([]*info.TriggerWorkerInfo, error) {
	l, err := s.client.List(ctx, s.getKey("/"))
	if err != nil {
		return nil, err
	}
	var list []*info.TriggerWorkerInfo
	for _, v := range l {
		var tWorker info.TriggerWorkerInfo
		err = json.Unmarshal(v.Value, &tWorker)
		if err != nil {
			return nil, errors.ErrJsonUnMarshal.Wrap(err)
		}
		tWorker.Id = filepath.Base(v.Key)
		list = append(list, &tWorker)
	}
	return list, nil
}
