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

package timingwheel

import (
	"container/list"
	"context"
	"encoding/json"
	stderr "errors"
	"fmt"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/errors"

	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/timer/metadata"
)

const (
	timerBuiltInEventbusReceivingStation    = "__Timer_RS"
	timerBuiltInEventbusDistributionStation = "__Timer_DS"
	timerBuiltInEventbus                    = "__Timer_%d_%d"
	xVanusEventbus                          = "xvanuseventbus"
	xVanusDeliveryTime                      = "xvanusdeliverytime"
	sleepDuration                           = 100 * time.Millisecond
)

type timingMsg struct {
	expiration time.Time
	event      *ce.Event
}

func newTimingMsg(ctx context.Context, e *ce.Event) *timingMsg {
	var (
		err        error
		expiration time.Time
	)
	extensions := e.Extensions()
	if deliveryTime, ok := extensions[xVanusDeliveryTime]; ok {
		expiration, err = types.ParseTime(deliveryTime.(string))
		if err != nil {
			log.Error(ctx, "parse time failed", map[string]interface{}{
				log.KeyError: err,
				"time":       deliveryTime,
			})
			expiration = time.Now()
		}
	} else {
		log.Error(ctx, "xvanusdeliverytime not found, set to current time", nil)
		expiration = time.Now()
	}
	return &timingMsg{
		expiration: expiration,
		event:      e,
	}
}

func (tm *timingMsg) hasExpired() bool {
	return !time.Now().Before(tm.expiration)
}

func (tm *timingMsg) getExpiration() time.Time {
	return tm.expiration
}

func (tm *timingMsg) getEvent() *ce.Event {
	return tm.event
}

type bucket struct {
	config   *Config
	tick     time.Duration
	interval time.Duration
	layer    int64
	slot     int64
	offset   int64
	eventbus string

	mu             sync.Mutex
	wg             sync.WaitGroup
	exitC          chan struct{}
	kvStore        kv.Client
	client         client.Client
	eventbusWriter api.BusWriter
	eventbusReader api.BusReader

	timingwheel *timingWheel
	element     *list.Element

	waitingForReady func(ctx context.Context, events []*ce.Event)
	eventHandler    func(ctx context.Context, event *ce.Event)
}

func newBucket(tw *timingWheel, element *list.Element, tick time.Duration, ebName string, layer, slot int64) *bucket {
	b := &bucket{
		config:      tw.config,
		tick:        tick,
		layer:       layer,
		slot:        slot,
		offset:      0,
		interval:    tick * time.Duration(tw.config.WheelSize),
		eventbus:    ebName,
		exitC:       make(chan struct{}),
		kvStore:     tw.kvStore,
		client:      tw.client,
		timingwheel: tw,
		element:     element,
	}

	if layer == 1 {
		b.waitingForReady = b.waitingForExpired
		b.eventHandler = b.pushToDistributionStation
	} else {
		b.waitingForReady = b.waitingForFlow
		b.eventHandler = b.pushToPrevTimingWheel
	}
	return b
}

func (b *bucket) start(ctx context.Context) error {
	var err error
	if err = b.createEventbus(ctx); err != nil {
		return err
	}

	b.connectEventbus(ctx)
	b.run(ctx)
	return nil
}

func (b *bucket) stop(ctx context.Context) {
	close(b.exitC)
	b.wait(ctx)
}

func (b *bucket) run(ctx context.Context) {
	offsetC := make(chan waitGroup, defaultMaxNumberOfWorkers)
	b.wg.Add(1)
	// update offset asynchronously
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at bucket update offset metadata", nil)
				return
			case <-b.exitC:
				log.Debug(ctx, "bucket exit at bucket update offset metadata", nil)
				return
			case offset := <-offsetC:
				// wait for all goroutines to finish before updating offset metadata
				offset.wg.Wait()
				log.Debug(ctx, "update offset metadata", map[string]interface{}{
					"eventbus":  b.eventbus,
					"update_to": offset.data,
				})
				b.updateOffsetMeta(ctx, offset.data)
			}
		}
	}()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		// limit the number of goroutines to no more than defaultMaxNumberOfWorkers
		glimitC := make(chan struct{}, defaultMaxNumberOfWorkers)
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at bucket running", map[string]interface{}{
					"eventbus": b.getEventbus(),
				})
				return
			case <-b.exitC:
				log.Debug(ctx, "bucket exit at bucket running", map[string]interface{}{
					"eventbus": b.getEventbus(),
				})
				return
			default:
				// batch read
				events, err := b.getEvent(ctx, defaultNumberOfEventsRead)
				if err != nil {
					if !errors.Is(err, errors.ErrOffsetOnEnd) && !errors.Is(err, errors.ErrTryAgain) {
						log.Error(ctx, "get event failed when bucket running", map[string]interface{}{
							"eventbus":   b.getEventbus(),
							log.KeyError: err,
						})
					}
					time.Sleep(sleepDuration)
					break
				}
				if len(events) == 0 {
					time.Sleep(sleepDuration)
					log.Debug(ctx, "no more message", map[string]interface{}{
						"function": "run",
					})
					break
				}
				// concurrent write
				numberOfEvents := int64(len(events))
				log.Debug(ctx, "got events when bucket running", map[string]interface{}{
					"eventbus":         b.eventbus,
					"offset":           b.offset,
					"layer":            b.layer,
					"number_of_events": numberOfEvents,
				})

				// block and wait here until events of the bucket ready to flow
				b.waitingForReady(ctx, events)

				wg := sync.WaitGroup{}
				for _, event := range events {
					wg.Add(1)
					glimitC <- struct{}{}
					go func(ctx context.Context, e *ce.Event) {
						defer wg.Done()
						b.eventHandler(ctx, e)
						<-glimitC
					}(ctx, event)
				}

				// asynchronously update offset after the same batch of events are successfully written.
				offsetC <- waitGroup{
					wg:   &wg,
					data: b.offset + numberOfEvents,
				}
				b.incOffset(numberOfEvents)
			}
		}
	}()
}

func (b *bucket) pushToDistributionStation(ctx context.Context, e *ce.Event) {
	tm := newTimingMsg(ctx, e)
	waitCtx, cancel := context.WithCancel(ctx)
	wait.Until(func() {
		if b.timingwheel.getDistributionStation().push(ctx, tm) {
			cancel()
		} else {
			log.Warning(ctx, "push event to distribution station failed, retry until it succeed", map[string]interface{}{
				"eventbus":   b.eventbus,
				"event_id":   e.ID(),
				"expiration": tm.getExpiration().Format(time.RFC3339Nano),
			})
		}
	}, b.config.Tick/defaultCheckWaitingPeriodRatio, waitCtx.Done())
}

func (b *bucket) pushToPrevTimingWheel(ctx context.Context, e *ce.Event) {
	var handler func(ctx context.Context, tm *timingMsg) bool
	tm := newTimingMsg(ctx, e)
	if tm.hasExpired() {
		handler = b.timingwheel.getDistributionStation().push
	} else {
		handler = b.getTimingWheelElement().prev().flow
	}
	waitCtx, cancel := context.WithCancel(ctx)
	wait.Until(func() {
		if handler(ctx, tm) {
			cancel()
		} else {
			log.Warning(ctx, "push event failed, retry until it succeed", map[string]interface{}{
				"eventbus":   b.eventbus,
				"event_id":   e.ID(),
				"expiration": tm.getExpiration().Format(time.RFC3339Nano),
			})
		}
	}, b.config.Tick/defaultCheckWaitingPeriodRatio, waitCtx.Done())
}

func (b *bucket) waitingForExpired(ctx context.Context, events []*ce.Event) {
	tm := newTimingMsg(ctx, events[0])
	blockCtx, blockCancel := context.WithCancel(ctx)
	wait.Until(func() {
		if b.isReadyToDeliver(tm) {
			blockCancel()
		}
	}, b.tick/defaultFrequentCheckWaitingPeriodRatio, blockCtx.Done())
}

func (b *bucket) isReadyToDeliver(tm *timingMsg) bool {
	startTimeOfBucket := tm.getExpiration().UnixNano() -
		(tm.getExpiration().UnixNano() % b.tick.Nanoseconds())
	return time.Now().UnixNano() >= startTimeOfBucket
}

func (b *bucket) waitingForFlow(ctx context.Context, events []*ce.Event) {
	tm := newTimingMsg(ctx, events[0])
	blockCtx, blockCancel := context.WithCancel(ctx)
	wait.Until(func() {
		if b.isReadyToFlow(tm) {
			blockCancel()
		}
	}, b.tick/defaultFrequentCheckWaitingPeriodRatio, blockCtx.Done())
}

func (b *bucket) isReadyToFlow(tm *timingMsg) bool {
	startTimeOfBucket := tm.getExpiration().UnixNano() -
		(tm.getExpiration().UnixNano() % b.tick.Nanoseconds())
	advanceTimeOfFlow := defaultNumberOfTickFlowInAdvance * b.getTimingWheelElement().prev().tick
	return time.Now().Add(advanceTimeOfFlow).UnixNano() >= startTimeOfBucket
}

func (b *bucket) push(ctx context.Context, tm *timingMsg) bool {
	return b.putEvent(ctx, tm) == nil
}

func (b *bucket) createEventbus(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.isLeader() {
		return nil
	}
	return b.timingwheel.ctrl.EventbusService().CreateSystemEventbusIfNotExist(ctx, b.eventbus,
		"System Eventbus For Timing Service")
}

func (b *bucket) connectEventbus(ctx context.Context) {
	b.eventbusWriter = b.client.Eventbus(ctx, b.eventbus).Writer()
	b.eventbusReader = b.client.Eventbus(ctx, b.eventbus).Reader()
}

func (b *bucket) putEvent(ctx context.Context, tm *timingMsg) (err error) {
	defer func() {
		if errOfPanic := recover(); errOfPanic != nil {
			log.Warning(ctx, "panic when put event", map[string]interface{}{
				log.KeyError: errOfPanic,
			})
			err = stderr.New("panic when put event")
		}
	}()
	if !b.isLeader() {
		return nil
	}
	_, err = api.AppendOne(ctx, b.eventbusWriter, tm.getEvent())
	if err != nil {
		log.Error(ctx, "append event to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   b.eventbus,
			"expiration": tm.getExpiration().Format(time.RFC3339Nano),
		})
		return err
	}
	log.Debug(ctx, "put event success", map[string]interface{}{
		"event_id":   tm.event.ID(),
		"eventbus":   b.eventbus,
		"expiration": tm.getExpiration().Format(time.RFC3339Nano),
	})
	return err
}

func (b *bucket) getEvent(ctx context.Context, number int16) (events []*ce.Event, err error) {
	defer func() {
		if errOfPanic := recover(); errOfPanic != nil {
			log.Warning(ctx, "panic when get event", map[string]interface{}{
				log.KeyError: errOfPanic,
			})
			events = []*ce.Event{}
			err = errors.ErrOffsetOnEnd
		}
	}()
	if !b.isLeader() {
		// TODO(jiangkai): redesign here for reduce cpu overload, by jiangkai, 2022.09.16
		time.Sleep(time.Second)
		return []*ce.Event{}, errors.ErrOffsetOnEnd
	}
	ls, err := b.client.Eventbus(ctx, b.eventbus).ListLog(ctx)
	if err != nil {
		return []*ce.Event{}, err
	}

	readPolicy := option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], b.offset))
	events, _, _, err = api.Read(ctx, b.eventbusReader, readPolicy, option.WithBatchSize(int(number)))
	if err != nil {
		if !errors.Is(err, errors.ErrOffsetOnEnd) &&
			!stderr.Is(ctx.Err(), context.Canceled) {
			log.Error(ctx, "read failed", map[string]interface{}{
				log.KeyError: err,
				"offset":     b.offset,
			})
		}
		return nil, err
	}
	return events, err
}

func (b *bucket) updateOffsetMeta(ctx context.Context, offset int64) {
	if !b.isLeader() {
		return
	}
	key := fmt.Sprintf("%s/offset/%s", metadata.MetadataKeyPrefixInKVStore, b.eventbus)
	offsetMeta := &metadata.OffsetMeta{
		Layer:    b.layer,
		Slot:     b.slot,
		Offset:   offset,
		Eventbus: b.eventbus,
	}
	data, _ := json.Marshal(offsetMeta)
	err := b.kvStore.Set(ctx, key, data)
	if err != nil {
		log.Warning(ctx, "set offset metadata to kvstore failed", map[string]interface{}{
			log.KeyError: err,
			"key":        key,
			"slot":       b.slot,
			"layer":      b.layer,
			"offset":     offset,
			"eventbus":   b.eventbus,
		})
	}
}

func (b *bucket) existsOffsetMeta(ctx context.Context) (bool, error) {
	key := fmt.Sprintf("%s/offset/%s", metadata.MetadataKeyPrefixInKVStore, b.eventbus)
	return b.kvStore.Exists(ctx, key)
}

func (b *bucket) getOffsetMeta(ctx context.Context) (int64, error) {
	if !b.isLeader() {
		return -1, nil
	}
	key := fmt.Sprintf("%s/offset/%s", metadata.MetadataKeyPrefixInKVStore, b.eventbus)
	value, err := b.kvStore.Get(ctx, key)
	if err != nil {
		log.Warning(ctx, "get offset metadata from kvstore failed", map[string]interface{}{
			log.KeyError: err,
			"key":        key,
			"slot":       b.slot,
			"layer":      b.layer,
			"eventbus":   b.eventbus,
		})
		return -1, err
	}
	md := &metadata.OffsetMeta{}
	_ = json.Unmarshal(value, md)
	return md.Offset, nil
}

func (b *bucket) deleteOffsetMeta(ctx context.Context) error {
	if !b.isLeader() {
		return nil
	}
	key := fmt.Sprintf("%s/offset/%s", metadata.MetadataKeyPrefixInKVStore, b.eventbus)
	err := b.kvStore.Delete(ctx, key)
	if err != nil {
		log.Warning(ctx, "delete offset metadata to kvstore failed", map[string]interface{}{
			log.KeyError: err,
			"key":        key,
		})
		return err
	}
	return nil
}

func (b *bucket) hasOnEnd(ctx context.Context) bool {
	_, errOnEnd := b.getEvent(ctx, 1)
	if !errors.Is(errOnEnd, errors.ErrOffsetOnEnd) {
		return false
	}
	off, err := b.getOffsetMeta(ctx)
	if err != nil {
		return false
	}
	return off == b.offset
}

func (b *bucket) recycle(ctx context.Context) {
	_ = b.timingwheel.ctrl.EventbusService().Delete(ctx, b.eventbus)
	_ = b.deleteOffsetMeta(ctx)
}

func (b *bucket) wait(ctx context.Context) {
	b.wg.Wait()
}

func (b *bucket) isLeader() bool {
	return b.timingwheel.leader
}

func (b *bucket) getEventbus() string {
	return b.eventbus
}

func (b *bucket) getOffset() int64 {
	return b.offset
}

func (b *bucket) getTimingWheelElement() *timingWheelElement {
	return b.element.Value.(*timingWheelElement)
}

func (b *bucket) incOffset(diff int64) {
	b.offset += diff
}
