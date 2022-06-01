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

//go:generate mockgen -source=subscription.go  -destination=mock_subscription.go -package=storage
package storage

import (
	"context"
	"encoding/json"
	"path"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type SubscriptionStorage interface {
	CreateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error
	UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error
	DeleteSubscription(ctx context.Context, id vanus.ID) error
	GetSubscription(ctx context.Context, id vanus.ID) (*primitive.SubscriptionData, error)
	ListSubscription(ctx context.Context) ([]*primitive.SubscriptionData, error)
}

type subscriptionStorage struct {
	client kv.Client
}

func NewSubscriptionStorage(client kv.Client) SubscriptionStorage {
	return &subscriptionStorage{
		client: client,
	}
}

func (s *subscriptionStorage) getKey(subID vanus.ID) string {
	return path.Join(KeyPrefixSubscription.String(), subID.String())
}

func (s *subscriptionStorage) CreateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error {
	v, err := json.Marshal(sub)
	if err != nil {
		return errors.ErrJsonMarshal
	}
	err = s.client.Create(ctx, s.getKey(sub.ID), v)
	if err != nil {
		return err
	}
	return nil
}

func (s *subscriptionStorage) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error {
	v, err := json.Marshal(sub)
	if err != nil {
		return errors.ErrJsonMarshal
	}
	err = s.client.Update(ctx, s.getKey(sub.ID), v)
	if err != nil {
		return err
	}
	return nil
}

func (s *subscriptionStorage) DeleteSubscription(ctx context.Context, id vanus.ID) error {
	return s.client.Delete(ctx, s.getKey(id))
}

func (s *subscriptionStorage) GetSubscription(ctx context.Context, id vanus.ID) (*primitive.SubscriptionData, error) {
	v, err := s.client.Get(ctx, s.getKey(id))
	if err != nil {
		return nil, err
	}
	sub := &primitive.SubscriptionData{}
	err = json.Unmarshal(v, sub)
	if err != nil {
		return nil, errors.ErrJsonUnMarshal
	}
	return sub, nil
}

func (s *subscriptionStorage) ListSubscription(ctx context.Context) ([]*primitive.SubscriptionData, error) {
	l, err := s.client.List(ctx, KeyPrefixSubscription.String())
	if err != nil {
		return nil, err
	}
	list := make([]*primitive.SubscriptionData, 0)
	for _, v := range l {
		sub := &primitive.SubscriptionData{}
		err = json.Unmarshal(v.Value, sub)
		if err != nil {
			return nil, errors.ErrJsonUnMarshal
		}
		list = append(list, sub)
	}
	return list, nil
}
