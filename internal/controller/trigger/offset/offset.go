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

package offset

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

type Manager struct {
	subOffset map[string]*subscriptionOffset
	storage   storage.OffsetStorage
	lock      sync.Mutex
}

func NewOffsetManager(storage storage.OffsetStorage) *Manager {
	m := &Manager{
		subOffset: map[string]*subscriptionOffset{},
		storage:   storage,
	}
	return m
}

func (m *Manager) GetOffset(ctx context.Context, subId string) (info.ListOffsetInfo, error) {
	offsets, err := m.storage.GetOffset(ctx, subId)
	if err != nil {
		if err == kv.ErrorKeyNotFound {
			return info.ListOffsetInfo{}, nil
		}
		return nil, err
	}
	return offsets, nil
}

func (m *Manager) Offset(subInfo info.SubscriptionInfo) {
	subOffset := m.getSubscription(subInfo.SubId)
	subOffset.offset(subInfo.Offsets)
}

func (m *Manager) getSubscription(subId string) *subscriptionOffset {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub, exist := m.subOffset[subId]
	if !exist {
		sub = &subscriptionOffset{
			subId: subId,
		}
		m.subOffset[subId] = sub
	}
	return sub
}

func (m *Manager) RemoveRegisterSubscription(subId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.subOffset, subId)
}

func (m *Manager) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.commit(ctx)
			}
		}
	}()
}

func (m *Manager) commit(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, sub := range m.subOffset {
		err := sub.commitOffset(ctx, m.storage)
		if err != nil {
			log.Warning(ctx, "commit offset error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: sub.subId,
				"offset":              sub.infos,
			})
		}
	}
}

type subscriptionOffset struct {
	subId string
	infos info.ListOffsetInfo
	exist bool
	lock  sync.Mutex
}

func (o *subscriptionOffset) offset(infos info.ListOffsetInfo) {
	o.lock.Lock()
	o.lock.Unlock()
	o.infos = infos
}

func (o *subscriptionOffset) getOffsetInfo() info.ListOffsetInfo {
	o.lock.Lock()
	o.lock.Unlock()
	return o.infos
}

func (o *subscriptionOffset) commitOffset(ctx context.Context, storage storage.OffsetStorage) error {
	offsets := o.getOffsetInfo()
	if len(offsets) == 0 {
		return nil
	}
	//todo check no change no need to commit
	if !o.exist {
		_, err := storage.GetOffset(ctx, o.subId)
		if err != nil {
			if err == kv.ErrorKeyNotFound {
				err = storage.CreateOffset(ctx, o.subId, offsets)
				if err != nil {
					o.exist = true
					return nil
				}
				return err
			} else {
				return err
			}
		}
		o.exist = true
	}
	err := storage.UpdateOffset(ctx, o.subId, offsets)
	if err != nil {
		return err
	}
	log.Debug(ctx, "update offset", map[string]interface{}{
		log.KeyError: err,
		"offset":     offsets,
	})
	return nil
}
