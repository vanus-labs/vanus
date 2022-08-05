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

//go:generate mockgen -source=subscription.go  -destination=mock_subscription.go -package=subscription
package subscription

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	SaveOffset(ctx context.Context, id vanus.ID, offsets iInfo.ListOffsetInfo, commit bool) error
	GetOffset(ctx context.Context, id vanus.ID) (iInfo.ListOffsetInfo, error)
	ListSubscription(ctx context.Context) []*metadata.Subscription
	GetSubscription(ctx context.Context, id vanus.ID) *metadata.Subscription
	AddSubscription(ctx context.Context, subscription *metadata.Subscription) error
	UpdateSubscription(ctx context.Context, subscription *metadata.Subscription) error
	Heartbeat(ctx context.Context, id vanus.ID, addr string, time time.Time) error
	DeleteSubscription(ctx context.Context, id vanus.ID) error
	Init(ctx context.Context) error
	Start()
	Stop()
}

var (
	ErrSubscriptionNotExist = fmt.Errorf("subscription not exist")
)

type manager struct {
	storage         storage.Storage
	offsetManager   offset.Manager
	lock            sync.RWMutex
	subscriptionMap map[vanus.ID]*metadata.Subscription
}

func NewSubscriptionManager(storage storage.Storage) Manager {
	m := &manager{
		storage:         storage,
		subscriptionMap: map[vanus.ID]*metadata.Subscription{},
	}
	return m
}

func (m *manager) SaveOffset(ctx context.Context, id vanus.ID, offsets iInfo.ListOffsetInfo, commit bool) error {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return nil
	}
	return m.offsetManager.Offset(ctx, id, offsets, commit)
}

func (m *manager) GetOffset(ctx context.Context, id vanus.ID) (iInfo.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return iInfo.ListOffsetInfo{}, ErrSubscriptionNotExist
	}
	return m.offsetManager.GetOffset(ctx, id)
}

func (m *manager) ListSubscription(ctx context.Context) []*metadata.Subscription {
	m.lock.RLock()
	defer m.lock.RUnlock()
	list := make([]*metadata.Subscription, 0, len(m.subscriptionMap))
	for _, subscription := range m.subscriptionMap {
		list = append(list, subscription)
	}
	return list
}

func (m *manager) GetSubscription(ctx context.Context, id vanus.ID) *metadata.Subscription {
	m.lock.RLock()
	defer m.lock.RUnlock()
	sub, exist := m.subscriptionMap[id]
	if !exist || sub.Phase == metadata.SubscriptionPhaseToDelete {
		return nil
	}
	return sub
}

func (m *manager) AddSubscription(ctx context.Context, sub *metadata.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	err := m.storage.CreateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	m.subscriptionMap[sub.ID] = sub
	return nil
}

func (m *manager) UpdateSubscription(ctx context.Context, sub *metadata.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.subscriptionMap[sub.ID]
	if !exist {
		return nil
	}
	err := m.storage.UpdateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSubscription will do
// 1.delete offset
// 2.delete subscription .
func (m *manager) DeleteSubscription(ctx context.Context, id vanus.ID) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.subscriptionMap[id]
	if !exist {
		return nil
	}
	err := m.offsetManager.RemoveRegisterSubscription(ctx, id)
	if err != nil {
		return err
	}
	err = m.storage.DeleteSubscription(ctx, id)
	if err != nil {
		return err
	}
	delete(m.subscriptionMap, id)
	return nil
}

func (m *manager) Heartbeat(ctx context.Context, id vanus.ID, addr string, time time.Time) error {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return ErrSubscriptionNotExist
	}
	if subscription.TriggerWorker != addr {
		// data is not consistent, record
		log.Error(ctx, "subscription trigger worker invalid", map[string]interface{}{
			log.KeySubscriptionID:    id,
			log.KeyTriggerWorkerAddr: subscription.TriggerWorker,
			"running_addr":           addr,
		})
	}
	subscription.HeartbeatTime = time
	return nil
}

func (m *manager) Stop() {
	m.offsetManager.Stop()
}

const (
	defaultCommitInterval = time.Second
)

func (m *manager) Init(ctx context.Context) error {
	subList, err := m.storage.ListSubscription(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "subscription size", map[string]interface{}{
		"size": len(subList),
	})
	for i := range subList {
		sub := subList[i]
		m.subscriptionMap[sub.ID] = sub
	}
	m.offsetManager = offset.NewOffsetManager(m.storage, defaultCommitInterval)
	return nil
}

func (m *manager) Start() {
	m.offsetManager.Start()
}
