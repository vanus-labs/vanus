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

//go:generate mockgen -source=offset.go -destination=mock_offset.go -package=offset
package offset

import (
	"context"
	"sync"
	"time"

	"github.com/vanus-labs/vanus/observability/log"

	"github.com/vanus-labs/vanus/internal/controller/trigger/storage"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

type Manager interface {
	GetOffset(ctx context.Context, subscriptionID vanus.ID) (info.ListOffsetInfo, error)
	Offset(ctx context.Context, subscriptionID vanus.ID, offsets info.ListOffsetInfo, commit bool) error
	RemoveRegisterSubscription(ctx context.Context, id vanus.ID) error
	Start()
	Stop()
}

const (
	defaultCommitInterval = time.Second
	defaultCloseWaitTime  = 2 * time.Second
)

type manager struct {
	subscriptionOffset sync.Map
	storage            storage.OffsetStorage
	commitInterval     time.Duration
	closeWaitTimeout   time.Duration
	ctx                context.Context
	stop               context.CancelFunc
	wg                 sync.WaitGroup
}

func NewOffsetManager(storage storage.OffsetStorage, commitInterval time.Duration) Manager {
	if commitInterval <= 0 {
		commitInterval = defaultCommitInterval
	}
	m := &manager{
		storage:          storage,
		commitInterval:   commitInterval,
		closeWaitTimeout: defaultCloseWaitTime,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) GetOffset(ctx context.Context, subscriptionID vanus.ID) (info.ListOffsetInfo, error) {
	subOffset, err := m.getSubscriptionOffset(ctx, subscriptionID)
	if err != nil {
		return nil, err
	}
	return subOffset.getOffsets(), nil
}

func (m *manager) Offset(ctx context.Context, subscriptionID vanus.ID, offsets info.ListOffsetInfo, commit bool) error {
	subOffset, err := m.getSubscriptionOffset(ctx, subscriptionID)
	if err != nil {
		return err
	}
	subOffset.offset(offsets)
	if commit {
		subOffset.commitOffset(ctx, m.storage)
	}
	return nil
}

func (m *manager) getSubscriptionOffset(ctx context.Context, id vanus.ID) (*subscriptionOffset, error) {
	subOffset, exist := m.subscriptionOffset.Load(id)
	if !exist {
		sub, err := initSubscriptionOffset(ctx, m.storage, id)
		if err != nil {
			return nil, err
		}
		subOffset, _ = m.subscriptionOffset.LoadOrStore(id, sub)
	}
	return subOffset.(*subscriptionOffset), nil
}

func (m *manager) RemoveRegisterSubscription(ctx context.Context, id vanus.ID) error {
	subOffset, exist := m.subscriptionOffset.Load(id)
	if exist {
		// stop commit
		subOffset.(*subscriptionOffset).stop()
		m.subscriptionOffset.Delete(id)
	}
	return m.storage.DeleteOffset(ctx, id)
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
				ctx, cancel := context.WithTimeout(context.Background(), m.closeWaitTimeout)
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
	m.subscriptionOffset.Range(func(key, value interface{}) bool {
		_subscriptionOffset, _ := value.(*subscriptionOffset)
		wg.Add(1)
		go func(_subscriptionOffset *subscriptionOffset) {
			defer wg.Done()
			_subscriptionOffset.commitOffset(ctx, m.storage)
		}(_subscriptionOffset)
		return true
	})
	wg.Wait()
}

type subscriptionOffset struct {
	subscriptionID vanus.ID
	offsets        sync.Map
	stopped        bool
	lock           sync.Mutex
}

func initSubscriptionOffset(ctx context.Context,
	storage storage.OffsetStorage,
	subscriptionID vanus.ID,
) (*subscriptionOffset, error) {
	list, err := storage.GetOffsets(ctx, subscriptionID)
	if err != nil {
		return nil, err
	}
	subOffset := &subscriptionOffset{
		subscriptionID: subscriptionID,
	}
	for _, o := range list {
		subOffset.offsets.Store(o.EventlogID, &eventlogOffset{
			subscriptionID: subscriptionID,
			eventlogID:     o.EventlogID,
			offset:         o.Offset,
			commit:         o.Offset,
			checkExist:     true,
		})
	}
	return subOffset, nil
}

// getEventlogOffset if not exist create.
func (o *subscriptionOffset) getEventlogOffset(info info.OffsetInfo) *eventlogOffset {
	elOffset, exist := o.offsets.Load(info.EventlogID)
	if !exist {
		elOffset = &eventlogOffset{
			subscriptionID: o.subscriptionID,
			eventlogID:     info.EventlogID,
			offset:         info.Offset,
		}
		elOffset, _ = o.offsets.LoadOrStore(info.EventlogID, elOffset)
	}
	return elOffset.(*eventlogOffset)
}

func (o *subscriptionOffset) offset(infos info.ListOffsetInfo) {
	for _, offset := range infos {
		elOffset := o.getEventlogOffset(offset)
		elOffset.setOffset(offset.Offset)
	}
}

func (o *subscriptionOffset) getOffsets() info.ListOffsetInfo {
	var offsets info.ListOffsetInfo
	o.offsets.Range(func(key, value interface{}) bool {
		elOffset, _ := value.(*eventlogOffset)
		offsets = append(offsets, info.OffsetInfo{
			EventlogID: elOffset.eventlogID,
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
		elOffset, _ := value.(*eventlogOffset)
		err := elOffset.commitOffset(ctx, storage)
		if err != nil {
			log.Warning(ctx, "commit offset fail", map[string]interface{}{
				log.KeySubscriptionID: o.subscriptionID,
				log.KeyEventlogID:     elOffset.eventlogID,
				"offset":              elOffset.offset,
				log.KeyError:          err,
			})
		}
		return true
	})
}

type eventlogOffset struct {
	subscriptionID vanus.ID
	eventlogID     vanus.ID
	offset         uint64
	commit         uint64
	checkExist     bool
}

func (o *eventlogOffset) setOffset(offset uint64) {
	o.offset = offset
}

func (o *eventlogOffset) commitOffset(ctx context.Context, storage storage.OffsetStorage) error {
	offset := o.offset
	if !o.checkExist {
		err := storage.CreateOffset(ctx, o.subscriptionID, info.OffsetInfo{
			EventlogID: o.eventlogID,
			Offset:     offset,
		})
		if err != nil {
			return err
		}
		log.Info(ctx, "create offset", map[string]interface{}{
			log.KeySubscriptionID: o.subscriptionID,
			log.KeyEventlogID:     o.eventlogID,
			"offset":              offset,
		})
		o.checkExist = true
		o.commit = offset
		return nil
	}
	if o.commit == offset {
		return nil
	}
	err := storage.UpdateOffset(ctx, o.subscriptionID, info.OffsetInfo{
		EventlogID: o.eventlogID,
		Offset:     offset,
	})
	if err != nil {
		return err
	}
	o.commit = offset
	return nil
}
