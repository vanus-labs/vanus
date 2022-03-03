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
	"path"
)

type Storage interface {
	CreateSubscription(sub *primitive.Subscription) error
	DeleteSubscription(subId string) error
	GetSubscription(subId string) (*primitive.Subscription, error)
	ListSubscription() ([]*primitive.Subscription, error)
	DeleteOffset(subId string) error
	Close() error
}

type storage struct {
	client kv.Client
}

func NewSubscriptionStorage(config config.KvStorageConfig) (Storage, error) {
	client, err := etcd.NewEtcdClientV3(config.ServerList, config.KeyPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "new etcd client has error")
	}
	return &storage{
		client: client,
	}, nil
}

func (s *storage) Close() error {
	return s.client.Close()
}

func (s *storage) CreateSubscription(sub *primitive.Subscription) error {
	v, err := json.Marshal(sub)
	if err != nil {
		return errors.Wrap(err, "json marshal error")
	}
	key := path.Join(config.StorageSubscription.String(), sub.ID)
	err = s.client.Create(key, v)
	if err != nil {
		return errors.Wrap(err, "etcd create error")
	}
	return nil
}

func (s *storage) DeleteSubscription(subId string) error {
	key := path.Join(config.StorageSubscription.String(), subId)
	err := s.client.Delete(key)
	if err != nil {
		return errors.Wrap(err, "etcd delete error")
	}
	return nil
}

func (s *storage) GetSubscription(subId string) (*primitive.Subscription, error) {
	key := path.Join(config.StorageSubscription.String(), subId)
	v, err := s.client.Get(key)
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

func (s *storage) ListSubscription() ([]*primitive.Subscription, error) {
	key := path.Join(config.StorageSubscription.String(), "/")
	l, err := s.client.List(key)
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

func (s *storage) DeleteOffset(subId string) error {
	key := path.Join(config.StorageOffset.String(), subId)
	return s.client.Delete(key)
}
