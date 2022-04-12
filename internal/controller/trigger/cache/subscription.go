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

package cache

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive"
	"sync"
)

type SubscriptionCache struct {
	lock         sync.RWMutex
	storage      storage.SubscriptionStorage
	subscription map[string]*primitive.SubscriptionApi
}

func NewSubscriptionCache(storage storage.SubscriptionStorage) *SubscriptionCache {
	return &SubscriptionCache{
		storage:      storage,
		subscription: map[string]*primitive.SubscriptionApi{},
	}
}

func (c *SubscriptionCache) InitSubscription(ctx context.Context) error {
	subList, err := c.storage.ListSubscription(ctx)
	if err != nil {
		return err
	}
	for i := range subList {
		sub := subList[i]
		c.subscription[sub.ID] = sub
	}
	return nil
}
func (c *SubscriptionCache) ListSubscription(ctx context.Context) map[string]*primitive.SubscriptionApi {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.subscription
}

func (c *SubscriptionCache) AddSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	err := c.storage.CreateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	c.subscription[sub.ID] = sub
	return nil
}

func (c *SubscriptionCache) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	err := c.storage.UpdateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	c.subscription[sub.ID] = sub
	return nil
}

func (c *SubscriptionCache) GetSubscription(ctx context.Context, subId string) *primitive.SubscriptionApi {
	c.lock.RLock()
	defer c.lock.RUnlock()
	sub, exist := c.subscription[subId]
	if !exist || sub.Phase == primitive.SubscriptionPhaseToDelete {
		return nil
	}
	return sub
}

func (c *SubscriptionCache) RemoveSubscription(ctx context.Context, subId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, exist := c.subscription[subId]
	if !exist {
		return nil
	}
	err := c.storage.DeleteSubscription(ctx, subId)
	if err != nil {
		return err
	}
	delete(c.subscription, subId)
	return nil
}
