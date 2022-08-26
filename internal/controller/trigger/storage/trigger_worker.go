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

//go:generate mockgen -source=trigger_worker.go  -destination=mock_trigger_worker.go -package=storage
package storage

import (
	"context"
	"encoding/json"
	"path"
	"path/filepath"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
)

type TriggerWorkerStorage interface {
	SaveTriggerWorker(context.Context, metadata.TriggerWorkerInfo) error
	GetTriggerWorker(ctx context.Context, id string) (*metadata.TriggerWorkerInfo, error)
	DeleteTriggerWorker(ctx context.Context, id string) error
	ListTriggerWorker(ctx context.Context) ([]*metadata.TriggerWorkerInfo, error)
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
	return path.Join(KeyPrefixTriggerWorker.String(), id)
}

func (s *triggerWorkerStorage) SaveTriggerWorker(ctx context.Context, info metadata.TriggerWorkerInfo) error {
	key := s.getKey(info.ID)
	v, err := json.Marshal(info)
	if err != nil {
		return errors.ErrJSONMarshal.Wrap(err)
	}
	return s.client.Set(ctx, key, v)
}
func (s *triggerWorkerStorage) GetTriggerWorker(ctx context.Context, id string) (*metadata.TriggerWorkerInfo, error) {
	v, err := s.client.Get(ctx, s.getKey(id))
	if err != nil {
		return nil, err
	}
	var tWorker metadata.TriggerWorkerInfo
	err = json.Unmarshal(v, &tWorker)
	if err != nil {
		return nil, errors.ErrJSONUnMarshal.Wrap(err)
	}
	return &tWorker, nil
}

func (s *triggerWorkerStorage) DeleteTriggerWorker(ctx context.Context, id string) error {
	return s.client.Delete(ctx, s.getKey(id))
}

func (s *triggerWorkerStorage) ListTriggerWorker(ctx context.Context) ([]*metadata.TriggerWorkerInfo, error) {
	l, err := s.client.List(ctx, s.getKey("/"))
	if err != nil {
		return nil, err
	}
	list := make([]*metadata.TriggerWorkerInfo, 0)
	for _, v := range l {
		var tWorker metadata.TriggerWorkerInfo
		err = json.Unmarshal(v.Value, &tWorker)
		if err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}
		tWorker.ID = filepath.Base(v.Key)
		list = append(list, &tWorker)
	}
	return list, nil
}
