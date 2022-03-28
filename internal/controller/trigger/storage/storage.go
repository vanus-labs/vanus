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
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive"
)

type Storage interface {
	SubscriptionStorage
	OffsetStorage
	TriggerWorkerStorage
	Close()
}

type storage struct {
	SubscriptionStorage
	OffsetStorage
	TriggerWorkerStorage
	client kv.Client
}

func NewStorage(config primitive.KvStorageConfig) (Storage, error) {
	client, err := etcd.NewEtcdClientV3(config.ServerList, config.KeyPrefix)
	if err != nil {
		return nil, err
	}
	s := &storage{client: client}
	s.SubscriptionStorage = NewSubscriptionStorage(client)
	s.OffsetStorage = NewOffsetStorage(client)
	s.TriggerWorkerStorage = NewTriggerWorkerStorage(client)
	return s, nil
}

func (s *storage) Close() {
	s.client.Close()
}
