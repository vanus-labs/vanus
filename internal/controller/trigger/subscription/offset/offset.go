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
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	GetOffset(ctx context.Context, subId vanus.ID) (info.ListOffsetInfo, error)
	Offset(ctx context.Context, subId vanus.ID, offsets info.ListOffsetInfo) error
	RemoveRegisterSubscription(ctx context.Context, subId vanus.ID) error
	Start()
	Stop()
}

type manager struct {
	subOffset      sync.Map
	storage        storage.OffsetStorage
	commitInterval time.Duration
	ctx            context.Context
	stop           context.CancelFunc
	wg             sync.WaitGroup
}

func NewOffsetManager(storage storage.OffsetStorage, commitInterval time.Duration) Manager {
	if commitInterval <= 0 {
		commitInterval = time.Second
	}
	m := &manager{
		storage:        storage,
		commitInterval: commitInterval,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) GetOffset(ctx context.Context, subId vanus.ID) (info.ListOffsetInfo, error) {
	subOffset, err := m.getSubscriptionOffset(ctx, subId)
	if err != nil {
		return nil, err
	}
	return subOffset.getOffsets(), nil
}

func (m *manager) Offset(ctx context.Context, subId vanus.ID, offsets info.ListOffsetInfo) error {
	subOffset, err := m.getSubscriptionOffset(ctx, subId)
	if err != nil {
		return err
	}
	subOffset.offset(offsets)
	return nil
}

func (m *manager) getSubscriptionOffset(ctx context.Context, subId vanus.ID) (*subscriptionOffset, error) {
	subOffset, exist := m.subOffset.Load(subId)
	if !exist {
		sub, err := initSubscriptionOffset(ctx, m.storage, subId)
		if err != nil {
			return nil, err
		}
		subOffset, _ = m.subOffset.LoadOrStore(subId, sub)
	}
	return subOffset.(*subscriptionOffset), nil
}

func (m *manager) RemoveRegisterSubscription(ctx context.Context, subId vanus.ID) error {
	subOffset, exist := m.subOffset.Load(subId)
	if exist {
		//stop commit
		subOffset.(*subscriptionOffset).stop()
		m.subOffset.Delete(subId)
	}
	return m.storage.DeleteOffset(ctx, subId)
}

func (m *manager) Stop() {
	m.stop()
	m.wg.Wait()
}

func (m *manager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.commitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				m.commit(ctx)
				cancel()
				return
			case <-ticker.C:
				m.commit(m.ctx)
			}
		}
	}()
}

func (m *manager) commit(ctx context.Context) {
	var wg sync.WaitGroup
	m.subOffset.Range(func(key, value interface{}) bool {
		sub := value.(*subscriptionOffset)
		wg.Add(1)
		go func(sub *subscriptionOffset) {
			defer wg.Done()
			sub.commitOffset(ctx, m.storage)
		}(sub)
		return true
	})
	wg.Wait()
}

type subscriptionOffset struct {
	subId   vanus.ID
	offsets sync.Map
	stopped bool
	lock    sync.Mutex
}

func initSubscriptionOffset(ctx context.Context, storage storage.OffsetStorage, subId vanus.ID) (*subscriptionOffset, error) {
	list, err := storage.GetOffsets(ctx, subId)
	if err != nil {
		return nil, err
	}
	subOffset := &subscriptionOffset{
		subId: subId,
	}
	for _, o := range list {
		subOffset.offsets.Store(o.EventLogID, &eventLogOffset{
			subId:      subId,
			eventLog:   o.EventLogID,
			offset:     o.Offset,
			commit:     o.Offset,
			checkExist: true,
		})
	}
	return subOffset, nil
}

//getEventLogOffset if not exist create
func (o *subscriptionOffset) getEventLogOffset(info info.OffsetInfo) *eventLogOffset {
	elOffset, exist := o.offsets.Load(info.EventLogID)
	if !exist {
		elOffset = &eventLogOffset{
			subId:    o.subId,
			eventLog: info.EventLogID,
			offset:   info.Offset,
		}
		elOffset, _ = o.offsets.LoadOrStore(info.EventLogID, elOffset)
	}
	return elOffset.(*eventLogOffset)
}

func (o *subscriptionOffset) offset(infos info.ListOffsetInfo) {
	for _, offset := range infos {
		elOffset := o.getEventLogOffset(offset)
		elOffset.setOffset(offset.Offset)
	}
}

func (o *subscriptionOffset) getOffsets() info.ListOffsetInfo {
	var offsets info.ListOffsetInfo
	o.offsets.Range(func(key, value interface{}) bool {
		elOffset := value.(*eventLogOffset)
		offsets = append(offsets, info.OffsetInfo{
			EventLogID: elOffset.eventLog,
			Offset:     elOffset.offset,
		})
		return true
	})
	return offsets
}

func (o *subscriptionOffset) stop() {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.stopped = true
}

func (o *subscriptionOffset) commitOffset(ctx context.Context, storage storage.OffsetStorage) {
	o.offsets.Range(func(key, value interface{}) bool {
		o.lock.Lock()
		defer o.lock.Unlock()
		if o.stopped {
			return false
		}
		elOffset := value.(*eventLogOffset)
		err := elOffset.commitOffset(ctx, storage)
		if err != nil {
			log.Warning(ctx, "commit offset fail", map[string]interface{}{
				log.KeySubscriptionID: o.subId,
				log.KeyEventlogID:     elOffset.eventLog,
				"offset":              elOffset.offset,
				log.KeyError:          err,
			})
		}
		return true
	})
}

type eventLogOffset struct {
	subId      vanus.ID
	eventLog   vanus.ID
	offset     uint64
	commit     uint64
	checkExist bool
}

func (o *eventLogOffset) setOffset(offset uint64) {
	o.offset = offset
}

func (o *eventLogOffset) commitOffset(ctx context.Context, storage storage.OffsetStorage) error {
	offset := o.offset
	if !o.checkExist {
		err := storage.CreateOffset(ctx, o.subId, info.OffsetInfo{
			EventLogID: o.eventLog,
			Offset:     offset,
		})
		if err != nil {
			return err
		}
		log.Debug(ctx, "create offset", map[string]interface{}{
			log.KeySubscriptionID: o.subId,
			log.KeyEventlogID:     o.eventLog,
			"offset":              offset,
		})
		o.checkExist = true
		o.commit = offset
		return nil
	}
	if o.commit == offset {
		return nil
	}
	err := storage.UpdateOffset(ctx, o.subId, info.OffsetInfo{
		EventLogID: o.eventLog,
		Offset:     offset,
	})
	if err != nil {
		return err
	}
	o.commit = offset
	return nil
}
