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
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/pkg/errors"
	"path"
)

type SubscriptionStorage interface {
	CreateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error
	DeleteSubscription(ctx context.Context, subId string) error
	GetSubscription(ctx context.Context, subId string) (*primitive.SubscriptionApi, error)
	ListSubscription(ctx context.Context) ([]*primitive.SubscriptionApi, error)
}

type subscriptionStorage struct {
	client kv.Client
}

func NewSubscriptionStorage(client kv.Client) SubscriptionStorage {
	return &subscriptionStorage{
		client: client,
	}
}

func (s *subscriptionStorage) Close() error {
	return s.client.Close()
}

func (s *subscriptionStorage) CreateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	v, err := json.Marshal(sub)
	if err != nil {
		return errors.Wrap(err, "json marshal error")
	}
	key := path.Join(primitive.StorageSubscription.String(), sub.ID)
	err = s.client.Create(ctx, key, v)
	if err != nil {
		return errors.Wrap(err, "etcd create error")
	}
	return nil
}

func (s *subscriptionStorage) DeleteSubscription(ctx context.Context, subId string) error {
	key := path.Join(primitive.StorageSubscription.String(), subId)
	err := s.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, "etcd delete error")
	}
	return nil
}

func (s *subscriptionStorage) GetSubscription(ctx context.Context, subId string) (*primitive.SubscriptionApi, error) {
	key := path.Join(primitive.StorageSubscription.String(), subId)
	v, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "etcd get error")
	}
	sub := &primitive.SubscriptionApi{}
	err = json.Unmarshal(v, sub)
	if err != nil {
		return nil, errors.Wrapf(err, "%s json unmarshal error", string(v))
	}
	return sub, nil
}

func (s *subscriptionStorage) ListSubscription(ctx context.Context) ([]*primitive.SubscriptionApi, error) {
	key := path.Join(primitive.StorageSubscription.String(), "/")
	l, err := s.client.List(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "etcd list error")
	}
	var list []*primitive.SubscriptionApi
	for _, v := range l {
		sub := &primitive.SubscriptionApi{}
		err = json.Unmarshal(v.Value, sub)
		if err != nil {
			return nil, errors.Wrapf(err, "%s json unmarshal error", string(v.Value))
		}
		list = append(list, sub)
	}
	return list, nil
}
