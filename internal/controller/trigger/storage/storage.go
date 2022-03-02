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
	"encoding/json"
	"github.com/linkall-labs/vanus/config"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/pkg/errors"
)

type SubscriptionStorage interface {
	CreateSubscription(sub *primitive.Subscription) error
	DeleteSubscription(id string) error
	GetSubscription(id string) (*primitive.Subscription, error)
	ListSubscription() ([]*primitive.Subscription, error)
	Close() error
}

type subscriptionStorage struct {
	client kv.Client
}

func NewSubscriptionStorage(config config.KvStorageConfig) (SubscriptionStorage, error) {
	client, err := etcd.NewEtcdClientV3(config.ServerList, config.KeyPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "new etcd client has error")
	}
	return &subscriptionStorage{
		client: client,
	}, nil
}

func (s *subscriptionStorage) Close() error {
	return s.client.Close()
}

func (s *subscriptionStorage) CreateSubscription(sub *primitive.Subscription) error {
	v, err := json.Marshal(sub)
	if err != nil {
		return errors.Wrap(err, "json marshal error")
	}
	err = s.client.Create(sub.ID, v)
	if err != nil {
		return errors.Wrap(err, "etcd create error")
	}
	return nil
}

func (s *subscriptionStorage) DeleteSubscription(id string) error {
	err := s.client.Delete(id)
	if err != nil {
		return errors.Wrap(err, "etcd delete error")
	}
	return nil
}

func (s *subscriptionStorage) GetSubscription(id string) (*primitive.Subscription, error) {
	v, err := s.client.Get(id)
	if err != nil {
		return nil, errors.Wrap(err, "etcd get error")
	}
	sub := &primitive.Subscription{}
	err = json.Unmarshal(v, sub)
	if err != nil {
		return nil, errors.Wrapf(err, "%s json unmarshal error", string(v))
	}
	return sub, nil
}

func (s *subscriptionStorage) ListSubscription() ([]*primitive.Subscription, error) {
	l, err := s.client.List("/")
	if err != nil {
		return nil, errors.Wrap(err, "etcd list error")
	}
	var list []*primitive.Subscription
	for _, v := range l {
		sub := &primitive.Subscription{}
		err = json.Unmarshal(v.Value, sub)
		if err != nil {
			return nil, errors.Wrapf(err, "%s json unmarshal error", string(v.Value))
		}
		list = append(list, sub)
	}
	return list, nil
}
