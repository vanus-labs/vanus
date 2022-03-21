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

package consumer

import (
	"context"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/storage"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type EventLogOffset struct {
	sub      string
	eventLog map[string]*offsetTracker
	storage  storage.OffsetStorage
	stop     context.CancelFunc
	wg       sync.WaitGroup
	mutex    sync.Mutex
}

func NewEventLogOffset(sub string, storage storage.OffsetStorage) *EventLogOffset {
	elo := &EventLogOffset{
		sub:      sub,
		storage:  storage,
		eventLog: map[string]*offsetTracker{},
	}
	return elo
}

func (offset *EventLogOffset) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	offset.stop = cancel
	offset.wg.Add(1)
	go func() {
		defer offset.wg.Done()
		tk := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				tk.Stop()
				offset.commit()
				return
			case <-tk.C:
				offset.commit()
			}
		}
	}()
}

func (offset *EventLogOffset) Close() {
	offset.stop()
	offset.wg.Wait()
}

func (offset *EventLogOffset) RegisterEventLog(el string, beginOffset int64) (int64, error) {
	offset.mutex.Lock()
	defer offset.mutex.Unlock()
	ctx := context.Background()
	offsetV, err := offset.storage.GetOffset(ctx, &info.OffsetInfo{
		SubId:    offset.sub,
		EventLog: el,
	})
	if err != nil {
		if err == kv.ErrorKeyNotFound {
			offsetV = beginOffset
			if err = offset.storage.CreateOffset(ctx, &info.OffsetInfo{
				SubId:    offset.sub,
				EventLog: el,
				Offset:   beginOffset,
			}); err != nil {
				return 0, errors.Wrapf(err, "sub %s el %s create offset error", offset.sub, el)
			}
		} else {
			return 0, errors.Wrapf(err, "sub %s el %s get offset error", offset.sub, el)
		}
	}
	offset.eventLog[el] = initOffset(offsetV)
	return offsetV, nil
}

func (offset *EventLogOffset) EventReceive(record *info.EventRecord) {
	if r, ok := offset.eventLog[record.EventLog]; !ok {
		return
	} else {
		r.putOffset(record.Offset)
	}
}

func (offset *EventLogOffset) EventCommit(record *info.EventRecord) {
	if r, ok := offset.eventLog[record.EventLog]; !ok {
		return
	} else {
		r.commitOffset(record.Offset)
	}
}

func (offset *EventLogOffset) commit() {
	offset.mutex.Lock()
	defer offset.mutex.Unlock()
	for k, o := range offset.eventLog {
		c := o.offsetToCommit()
		if c <= o.getCommit() {
			continue
		}
		o.setCommit(c)
		ctx := context.Background()
		log.Debug(ctx, "commit offset", map[string]interface{}{"sub": offset.sub, "el": k, "offset": c})
		err := offset.storage.UpdateOffset(context.Background(), &info.OffsetInfo{
			SubId:    offset.sub,
			EventLog: k,
			Offset:   c,
		})
		if err != nil {
			log.Error(ctx, "commit offset failed", map[string]interface{}{
				"sub": offset.sub, "el": k, "offset": c, "error": err,
			})
		}
	}
}

type offsetTracker struct {
	mutex     sync.Mutex
	commit    int64
	maxOffset int64
	list      *skiplist.SkipList
}

func initOffset(initOffset int64) *offsetTracker {
	return &offsetTracker{
		commit:    initOffset,
		maxOffset: initOffset,
		list: skiplist.New(skiplist.GreaterThanFunc(func(lhs, rhs interface{}) int {
			v1 := lhs.(int64)
			v2 := rhs.(int64)
			if v1 > v2 {
				return 1
			} else if v1 < v2 {
				return -1
			}
			return 0
		})),
	}
}

func (o *offsetTracker) putOffset(offset int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Set(offset, offset)
	o.maxOffset = o.list.Back().Key().(int64)
}

func (o *offsetTracker) commitOffset(offset int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Remove(offset)
}

func (o *offsetTracker) setCommit(commit int64) {
	o.commit = commit
}

func (o *offsetTracker) getCommit() int64 {
	return o.commit
}

func (o *offsetTracker) offsetToCommit() int64 {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.list.Len() == 0 {
		return o.maxOffset
	}
	return o.list.Front().Key().(int64)
}
