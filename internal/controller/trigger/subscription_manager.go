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

package trigger

import (
	"context"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/trigger/cache"
	"github.com/linkall-labs/vanus/internal/controller/trigger/offset"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
)

type SubscriptionManager struct {
	storage           storage.Storage
	subscriptionCache *cache.SubscriptionCache
	offsetManager     *offset.Manager
	ctx               context.Context
	stop              context.CancelFunc
}

func NewSubscriptionManager(storage storage.Storage) *SubscriptionManager {
	m := &SubscriptionManager{
		storage: storage,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *SubscriptionManager) Offset(ctx context.Context, subInfo iInfo.SubscriptionInfo) error {
	return m.offsetManager.Offset(ctx, subInfo)
}

func (m *SubscriptionManager) GetOffset(ctx context.Context, subId string) (iInfo.ListOffsetInfo, error) {
	return m.offsetManager.GetOffset(ctx, subId)
}

func (m *SubscriptionManager) ListSubscription(ctx context.Context) map[string]*primitive.SubscriptionApi {
	return m.subscriptionCache.ListSubscription(ctx)
}

func (m *SubscriptionManager) GetSubscription(ctx context.Context, subId string) *primitive.SubscriptionApi {
	return m.subscriptionCache.GetSubscription(ctx, subId)
}

func (m *SubscriptionManager) AddSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	sub.ID = uuid.NewString()
	sub.Phase = primitive.SubscriptionPhaseCreated
	err := m.subscriptionCache.AddSubscription(ctx, sub)
	if err != nil {
		return err
	}
	return nil
}

func (m *SubscriptionManager) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	return m.subscriptionCache.UpdateSubscription(ctx, sub)
}

//DeleteSubscription will do
//1.delete offset
//2.delete subscription
func (m *SubscriptionManager) DeleteSubscription(ctx context.Context, subId string) error {
	err := m.offsetManager.RemoveRegisterSubscription(ctx, subId)
	if err != nil {
		return err
	}
	err = m.subscriptionCache.RemoveSubscription(ctx, subId)
	if err != nil {
		return err
	}
	return nil
}

func (m *SubscriptionManager) Stop() {
	m.stop()
	m.offsetManager.Stop()
}

func (m *SubscriptionManager) Init(ctx context.Context) error {
	m.subscriptionCache = cache.NewSubscriptionCache(m.storage)
	err := m.subscriptionCache.InitSubscription(ctx)
	if err != nil {
		return err
	}
	m.offsetManager = offset.NewOffsetManager(m.storage)
	return nil
}

func (m *SubscriptionManager) Run() {
	m.offsetManager.Run()
}
