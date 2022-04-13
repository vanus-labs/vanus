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

//go:generate mockgen -source=subscription_manager.go  -destination=testing/mock_subscription_manager.go -package=testing
package subscription

import (
	"context"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/linkall-labs/vanus/internal/primitive"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"sync"
)

type Manager interface {
	Offset(ctx context.Context, subInfo iInfo.SubscriptionInfo) error
	GetOffset(ctx context.Context, subId string) (iInfo.ListOffsetInfo, error)
	ListSubscription(ctx context.Context) map[string]*primitive.SubscriptionApi
	GetSubscription(ctx context.Context, subId string) *primitive.SubscriptionApi
	AddSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error
	UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error
	DeleteSubscription(ctx context.Context, subId string) error
	Init(ctx context.Context) error
	Start()
	Stop()
}

type manager struct {
	storage       storage.Storage
	offsetManager offset.Manager
	ctx           context.Context
	stop          context.CancelFunc
	lock          sync.RWMutex
	subscription  map[string]*primitive.SubscriptionApi
}

func NewSubscriptionManager(storage storage.Storage) Manager {
	m := &manager{
		storage:      storage,
		subscription: map[string]*primitive.SubscriptionApi{},
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) Offset(ctx context.Context, subInfo iInfo.SubscriptionInfo) error {
	return m.offsetManager.Offset(ctx, subInfo)
}

func (m *manager) GetOffset(ctx context.Context, subId string) (iInfo.ListOffsetInfo, error) {
	return m.offsetManager.GetOffset(ctx, subId)
}

func (m *manager) ListSubscription(ctx context.Context) map[string]*primitive.SubscriptionApi {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.subscription
}

func (m *manager) GetSubscription(ctx context.Context, subId string) *primitive.SubscriptionApi {
	m.lock.RLock()
	defer m.lock.RUnlock()
	sub, exist := m.subscription[subId]
	if !exist || sub.Phase == primitive.SubscriptionPhaseToDelete {
		return nil
	}
	return sub
}

func (m *manager) AddSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub.ID = uuid.NewString()
	sub.Phase = primitive.SubscriptionPhaseCreated
	err := m.storage.CreateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	m.subscription[sub.ID] = sub
	return nil
}

func (m *manager) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	err := m.storage.UpdateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	m.subscription[sub.ID] = sub
	return nil
}

//DeleteSubscription will do
//1.delete offset
//2.delete subscription
func (m *manager) DeleteSubscription(ctx context.Context, subId string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.subscription[subId]
	if !exist {
		return nil
	}
	err := m.offsetManager.RemoveRegisterSubscription(ctx, subId)
	if err != nil {
		return err
	}
	err = m.storage.DeleteSubscription(ctx, subId)
	if err != nil {
		return err
	}
	delete(m.subscription, subId)
	return nil
}

func (m *manager) Stop() {
	m.stop()
	m.offsetManager.Stop()
}

func (m *manager) Init(ctx context.Context) error {
	subList, err := m.storage.ListSubscription(ctx)
	if err != nil {
		return err
	}
	for i := range subList {
		sub := subList[i]
		m.subscription[sub.ID] = sub
	}
	m.offsetManager = offset.NewOffsetManager(m.storage)
	return nil
}

func (m *manager) Start() {
	m.offsetManager.Start()
}
