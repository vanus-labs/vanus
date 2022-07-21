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

//go:generate mockgen -source=manager.go  -destination=mock_manager.go -package=worker
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	AddSubscription(ctx context.Context, subscription *primitive.Subscription) error
	RemoveSubscription(ctx context.Context, id vanus.ID) error
	PauseSubscription(ctx context.Context, id vanus.ID) error
	ListSubscriptionInfo() ([]pInfo.SubscriptionInfo, func())
}

const (
	defaultCleanSubscriptionTimeout = 5 * time.Second
	cleanSubscriptionCheckPeriod    = 10 * time.Millisecond
)

type newSubscriptionWorker func(subscription *primitive.Subscription,
	subscriptionOffset *offset.SubscriptionOffset,
	config Config) SubscriptionWorker

type manager struct {
	subscriptionMap       sync.Map
	offsetManager         *offset.Manager
	ctx                   context.Context
	stop                  context.CancelFunc
	config                Config
	newSubscriptionWorker newSubscriptionWorker
}

func NewManager(config Config) Manager {
	if config.CleanSubscriptionTimeout == 0 {
		config.CleanSubscriptionTimeout = defaultCleanSubscriptionTimeout
	}
	m := &manager{
		config:                config,
		newSubscriptionWorker: NewSubscriptionWorker,
		offsetManager:         offset.NewOffsetManager(),
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) getSubscriptionWorker(id vanus.ID) SubscriptionWorker {
	v, exist := m.subscriptionMap.Load(id)
	if !exist {
		return nil
	}
	worker, _ := v.(SubscriptionWorker)
	return worker
}

func (m *manager) Start(ctx context.Context) error {
	return nil
}

func (m *manager) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	m.subscriptionMap.Range(func(key, value interface{}) bool {
		wg.Add(1)
		id, _ := key.(vanus.ID)
		go func(id vanus.ID) {
			defer wg.Done()
			m.stopSubscription(m.ctx, id)
			m.cleanSubscription(m.ctx, id)
		}(id)
		return true
	})
	wg.Wait()
	m.stop()
	return nil
}

func (m *manager) AddSubscription(ctx context.Context, subscription *primitive.Subscription) error {
	data, exist := m.subscriptionMap.Load(subscription.ID)
	if exist {
		worker, _ := data.(SubscriptionWorker)
		err := worker.Change(ctx, subscription)
		return err
	}
	subOffset := m.offsetManager.RegisterSubscription(subscription.ID)
	worker := m.newSubscriptionWorker(subscription, subOffset, m.config)
	m.subscriptionMap.Store(subscription.ID, worker)
	err := worker.Run(m.ctx)
	if err != nil {
		m.subscriptionMap.Delete(subscription.ID)
		m.offsetManager.RemoveSubscription(subscription.ID)
		return err
	}
	return nil
}

func (m *manager) RemoveSubscription(ctx context.Context, id vanus.ID) error {
	_, exist := m.subscriptionMap.Load(id)
	if !exist {
		return nil
	}
	m.stopSubscription(ctx, id)
	m.cleanSubscription(m.ctx, id)
	return nil
}

func (m *manager) PauseSubscription(ctx context.Context, id vanus.ID) error {
	m.stopSubscription(ctx, id)
	return nil
}

func (m *manager) ListSubscriptionInfo() ([]pInfo.SubscriptionInfo, func()) {
	list := make([]pInfo.SubscriptionInfo, 0)
	m.subscriptionMap.Range(func(key, value interface{}) bool {
		id, _ := key.(vanus.ID)
		subOffset := m.offsetManager.GetSubscription(id)
		if subOffset == nil {
			return true
		}
		list = append(list, pInfo.SubscriptionInfo{
			SubscriptionID: id,
			Offsets:        subOffset.GetCommit(),
		})
		return true
	})
	return list, func() {
		m.offsetManager.SetLastCommitTime()
	}
}

func (m *manager) stopSubscription(ctx context.Context, id vanus.ID) {
	value, exist := m.subscriptionMap.Load(id)
	if !exist {
		return
	}
	worker, _ := value.(SubscriptionWorker)
	worker.Stop(ctx)
	log.Info(ctx, "stop subscription success", map[string]interface{}{
		log.KeySubscriptionID: id,
	})
}

func (m *manager) cleanSubscription(ctx context.Context, id vanus.ID) {
	info, exist := m.subscriptionMap.Load(id)
	if !exist {
		return
	}
	worker, _ := info.(SubscriptionWorker)
	if !worker.IsStart() {
		m.subscriptionMap.Delete(id)
		m.offsetManager.RemoveSubscription(id)
		return
	}
	// wait offset commit or timeout .
	ctx, cancel := context.WithTimeout(ctx, m.config.CleanSubscriptionTimeout)
	defer cancel()
	ticker := time.NewTicker(cleanSubscriptionCheckPeriod)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			if worker.GetStopTime().Before(m.offsetManager.GetLastCommitTime()) {
				break loop
			}
		}
	}
	m.subscriptionMap.Delete(id)
	m.offsetManager.RemoveSubscription(id)
}
