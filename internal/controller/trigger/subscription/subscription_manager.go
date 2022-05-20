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

//go:generate mockgen -source=subscription_manager.go  -destination=mock_subscription_manager.go -package=subscription
package subscription

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/linkall-labs/vanus/internal/primitive"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	Offset(ctx context.Context, subId vanus.ID, offsets iInfo.ListOffsetInfo) error
	GetOffset(ctx context.Context, subId vanus.ID) (iInfo.ListOffsetInfo, error)
	ListSubscription(ctx context.Context) map[vanus.ID]*primitive.SubscriptionData
	GetSubscriptionData(ctx context.Context, subId vanus.ID) *primitive.SubscriptionData
	GetSubscription(ctx context.Context, subId vanus.ID) (*primitive.Subscription, error)
	AddSubscription(ctx context.Context, sub *primitive.SubscriptionData) error
	UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error
	Heartbeat(ctx context.Context, subId vanus.ID, addr string, time time.Time) error
	DeleteSubscription(ctx context.Context, subId vanus.ID) error
	Init(ctx context.Context) error
	Start()
	Stop()
}

var (
	ErrSubscriptionNotExist = fmt.Errorf("subscription not exist")
)

type manager struct {
	storage       storage.Storage
	offsetManager offset.Manager
	ctx           context.Context
	stop          context.CancelFunc
	lock          sync.RWMutex
	subscription  map[vanus.ID]*primitive.SubscriptionData
}

func NewSubscriptionManager(storage storage.Storage) Manager {
	m := &manager{
		storage:      storage,
		subscription: map[vanus.ID]*primitive.SubscriptionData{},
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) Offset(ctx context.Context, subId vanus.ID, offsets iInfo.ListOffsetInfo) error {
	subData := m.GetSubscriptionData(ctx, subId)
	if subData == nil {
		return nil
	}
	return m.offsetManager.Offset(ctx, subId, offsets)
}

func (m *manager) GetOffset(ctx context.Context, subId vanus.ID) (iInfo.ListOffsetInfo, error) {
	subData := m.GetSubscriptionData(ctx, subId)
	if subData == nil {
		return iInfo.ListOffsetInfo{}, nil
	}
	return m.offsetManager.GetOffset(ctx, subId)
}

func (m *manager) ListSubscription(ctx context.Context) map[vanus.ID]*primitive.SubscriptionData {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.subscription
}

func (m *manager) GetSubscriptionData(ctx context.Context, subId vanus.ID) *primitive.SubscriptionData {
	m.lock.RLock()
	defer m.lock.RUnlock()
	sub, exist := m.subscription[subId]
	if !exist || sub.Phase == primitive.SubscriptionPhaseToDelete {
		return nil
	}
	return sub
}

func (m *manager) GetSubscription(ctx context.Context, subId vanus.ID) (*primitive.Subscription, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	subData, exist := m.subscription[subId]
	if !exist || subData.Phase == primitive.SubscriptionPhaseToDelete {
		return nil, ErrSubscriptionNotExist
	}
	offsets, err := m.offsetManager.GetOffset(ctx, subId)
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
	}
	return sub, nil
}

func (m *manager) AddSubscription(ctx context.Context, sub *primitive.SubscriptionData) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub.ID = vanus.GenerateID()
	sub.Phase = primitive.SubscriptionPhaseCreated
	err := m.storage.CreateSubscription(ctx, sub)
	if err != nil {
		return err
	}
	m.subscription[sub.ID] = sub
	return nil
}

func (m *manager) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionData) error {
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
func (m *manager) DeleteSubscription(ctx context.Context, subId vanus.ID) error {
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

func (m *manager) Heartbeat(ctx context.Context, subId vanus.ID, addr string, time time.Time) error {
	subData := m.GetSubscriptionData(ctx, subId)
	if subData == nil {
		return ErrSubscriptionNotExist
	}
	if subData.TriggerWorker != addr {
		//数据不一致了，有bug了
		log.Error(ctx, "subscription trigger worker invalid", map[string]interface{}{
			log.KeySubscriptionID:    subId,
			log.KeyTriggerWorkerAddr: subData.TriggerWorker,
			"running_addr":           addr,
		})
	}
	if subData.Phase != primitive.SubscriptionPhaseRunning {
		subData.Phase = primitive.SubscriptionPhaseRunning
		err := m.UpdateSubscription(ctx, subData)
		if err != nil {
			log.Error(ctx, "storage save subscription phase to running error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: subId,
			})
		}
	}
	subData.HeartbeatTime = time
	return nil
}

func (m *manager) Stop() {
	m.stop()
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
		m.subscription[sub.ID] = sub
	}
	m.offsetManager = offset.NewOffsetManager(m.storage, defaultCommitInterval)
	return nil
}

func (m *manager) Start() {
	m.offsetManager.Start()
}
