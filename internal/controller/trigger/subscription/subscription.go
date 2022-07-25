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
	"github.com/linkall-labs/vanus/internal/primitive"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	Offset(ctx context.Context, id vanus.ID, offsets iInfo.ListOffsetInfo) error
	GetOffset(ctx context.Context, id vanus.ID) (iInfo.ListOffsetInfo, error)
	ListSubscription(ctx context.Context) []*metadata.Subscription
	GetSubscription(ctx context.Context, id vanus.ID) *metadata.Subscription
	GetSubscriptionWithOffset(ctx context.Context, id vanus.ID) (*primitive.Subscription, error)
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

func (m *manager) Offset(ctx context.Context, id vanus.ID, offsets iInfo.ListOffsetInfo) error {
	subData := m.GetSubscription(ctx, id)
	if subData == nil {
		return nil
	}
	return m.offsetManager.Offset(ctx, id, offsets)
}

func (m *manager) GetOffset(ctx context.Context, id vanus.ID) (iInfo.ListOffsetInfo, error) {
	subData := m.GetSubscription(ctx, id)
	if subData == nil {
		return iInfo.ListOffsetInfo{}, nil
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

func (m *manager) GetSubscriptionWithOffset(ctx context.Context, id vanus.ID) (*primitive.Subscription, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	subData, exist := m.subscriptionMap[id]
	if !exist || subData.Phase == metadata.SubscriptionPhaseToDelete {
		return nil, ErrSubscriptionNotExist
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return nil, err
	}
	sub := &primitive.Subscription{
		ID:               subData.ID,
		Filters:          subData.Filters,
		Sink:             subData.Sink,
		EventBus:         subData.EventBus,
		Offsets:          offsets,
		InputTransformer: subData.InputTransformer,
		Config:           subData.Config,
	}
	return sub, nil
}

func (m *manager) AddSubscription(ctx context.Context, sub *metadata.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub.ID = vanus.NewID()
	sub.Phase = metadata.SubscriptionPhaseCreated
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
	err := m.storage.UpdateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	m.subscriptionMap[sub.ID] = sub
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
	subscriptionData := m.GetSubscription(ctx, id)
	if subscriptionData == nil {
		return ErrSubscriptionNotExist
	}
	if subscriptionData.TriggerWorker != addr {
		// 数据不一致了，有bug了
		log.Error(ctx, "subscription trigger worker invalid", map[string]interface{}{
			log.KeySubscriptionID:    id,
			log.KeyTriggerWorkerAddr: subscriptionData.TriggerWorker,
			"running_addr":           addr,
		})
	}
	if subscriptionData.Phase != metadata.SubscriptionPhaseRunning {
		subscriptionData.Phase = metadata.SubscriptionPhaseRunning
		err := m.UpdateSubscription(ctx, subscriptionData)
		if err != nil {
			log.Error(ctx, "storage save subscription phase to running error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: id,
			})
		}
	}
	subscriptionData.HeartbeatTime = time
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
