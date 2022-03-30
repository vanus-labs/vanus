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
	subOffset, err := m.getSubscriptionOffset(ctx, subId)
	if err != nil {
		return nil, err
	}
	return subOffset.getOffsets(), nil
}

func (m *Manager) Offset(ctx context.Context, subInfo info.SubscriptionInfo) error {
	subOffset, err := m.getSubscriptionOffset(ctx, subInfo.SubId)
	if err != nil {
		return err
	}
	subOffset.offset(subInfo.Offsets)
	return nil
}

func (m *Manager) getSubscriptionOffset(ctx context.Context, subId string) (*subscriptionOffset, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub, exist := m.subOffset[subId]
	if !exist {
		var err error
		sub, err = m.initSubscriptionOffset(ctx, subId)
		if err != nil {
			return nil, err
		}
		m.subOffset[subId] = sub
	}
	return sub, nil
}

func (m *Manager) initSubscriptionOffset(ctx context.Context, subId string) (*subscriptionOffset, error) {
	list, err := m.storage.GetOffset(ctx, subId)
	if err != nil {
		if err == kv.ErrorKeyNotFound {
			err = m.storage.CreateOffset(ctx, subId, info.ListOffsetInfo{})
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	offsets := make(map[string]*info.OffsetInfo, len(list))
	for _, o := range list {
		offsets[o.EventLog] = &info.OffsetInfo{
			EventLog: o.EventLog,
			Offset:   o.Offset,
		}
	}
	return &subscriptionOffset{
		subId:   subId,
		offsets: offsets,
	}, nil

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
				"offset":              sub.offsets,
			})
		}
	}
}

type subscriptionOffset struct {
	subId   string
	offsets map[string]*info.OffsetInfo
	change  bool
	lock    sync.Mutex
}

func (o *subscriptionOffset) offset(infos info.ListOffsetInfo) {
	o.lock.Lock()
	o.lock.Unlock()
	for _, info := range infos {
		now, exist := o.offsets[info.EventLog]
		if !exist || now.Offset != info.Offset {
			o.offsets[info.EventLog] = &info
			o.change = true
		}
	}
}

func (o *subscriptionOffset) getOffsets() info.ListOffsetInfo {
	var offsets info.ListOffsetInfo
	for _, v := range o.offsets {
		offsets = append(offsets, info.OffsetInfo{
			EventLog: v.EventLog,
			Offset:   v.Offset,
		})
	}
	return offsets
}

func (o *subscriptionOffset) commitOffset(ctx context.Context, storage storage.OffsetStorage) error {
	o.lock.Lock()
	o.lock.Unlock()
	if !o.change {
		return nil
	}
	offsets := o.getOffsets()
	if len(offsets) == 0 {
		return nil
	}
	err := storage.UpdateOffset(ctx, o.subId, offsets)
	if err != nil {
		return err
	}
	o.change = false
	log.Debug(ctx, "update offset", map[string]interface{}{
		log.KeySubscriptionID: o.subId,
		"offset":              offsets,
	})
	return nil
}
