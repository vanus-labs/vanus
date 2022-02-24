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
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

var (
	eventLogOffsets = make(map[string]*EventLogOffset)
)

func AddEventLogOffset(sub string, offset *EventLogOffset) {
	eventLogOffsets[sub] = offset
}

func RemoveEventLogOffset(sub string) {
	delete(eventLogOffsets, sub)
}

func GetEventLogOffset(sub string) *EventLogOffset {
	return eventLogOffsets[sub]
}

type EventLogOffset struct {
	sub      string
	ctx      context.Context
	eventLog map[string]*offsetTracker
	mutex    sync.Mutex
}

func NewEventLogOffset(ctx context.Context, sub string) *EventLogOffset {
	context.Background()
	elo := &EventLogOffset{
		sub:      sub,
		eventLog: map[string]*offsetTracker{},
		ctx:      ctx,
	}
	go elo.run()
	return elo
}

func (offset *EventLogOffset) run() {
	tk := time.NewTicker(time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-offset.ctx.Done():
			return
		case <-tk.C:
			offset.commit()
		}
	}
}

func (offset *EventLogOffset) RegisterEventLog(el string) (int64, error) {
	offset.mutex.Lock()
	offset.mutex.Unlock()
	//TODO get offset from etcd
	var offsetV int64
	offset.eventLog[el] = initOffset(offsetV)
	return offsetV, nil
}

func (offset *EventLogOffset) EventReceive(record *EventRecord) {
	if r, ok := offset.eventLog[record.EventLog]; !ok {
		return
	} else {
		r.commitOffset(record.Offset)
	}
}

func (offset *EventLogOffset) EventCommit(record *EventRecord) {
	if r, ok := offset.eventLog[record.EventLog]; !ok {
		return
	} else {
		r.commitOffset(record.Offset)
	}
}

func (offset *EventLogOffset) commit() {
	offset.mutex.Lock()
	offset.mutex.Unlock()
	for k, o := range offset.eventLog {
		c := o.offsetToCommit()
		if c <= o.commit {
			continue
		}
		o.setCommit(c)
		log.Debug("commit offset", map[string]interface{}{"sub": offset.sub, "el": k, "offset": c})
		//TODO commit to store
	}
}

type offsetTracker struct {
	initOffset int64
	commit     int64
	commitSet  *util.BitSet
	mutex      sync.Mutex
}

func initOffset(initOffset int64) *offsetTracker {
	return &offsetTracker{
		initOffset: initOffset,
		commit:     initOffset,
		commitSet:  util.NewBitSet(),
	}
}

func (o *offsetTracker) commitOffset(offset int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	bitSetOffset := int(offset - o.initOffset)
	o.commitSet.Set(bitSetOffset)
	o.reset(bitSetOffset)
}

func (o *offsetTracker) setCommit(commit int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.commit = commit
}

func (o *offsetTracker) offsetToCommit() int64 {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.initOffset + int64(o.commitSet.NextClearBit(int(o.commit-o.initOffset)))
}

func (o *offsetTracker) reset(offset int) {
	if offset <= 100000 {
		return
	}
	relativeOffset := o.commit - o.initOffset
	wordOfCommit := (int)(relativeOffset / 64)
	o.commitSet = o.commitSet.Clear(wordOfCommit)
	o.initOffset = o.commit
}
