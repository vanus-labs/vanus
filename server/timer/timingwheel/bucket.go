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
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/vanus-labs/vanus/api/errors"
	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/server/timer/metadata"
)

const (
	timerBuiltInEventbusReceivingStation    = "__Timer_RS"
	timerBuiltInEventbusDistributionStation = "__Timer_DS"
	timerBuiltInEventbus                    = "__Timer_%d_%d"
	xVanusEventbus                          = primitive.XVanusEventbus
	xVanusDeliveryTime                      = primitive.XVanusDeliveryTime
	sleepDuration                           = 100 * time.Millisecond
)

type timingMsg struct {
	expiration time.Time
	event      *ce.Event
}

func newTimingMsg(ctx context.Context, e *ce.Event) *timingMsg {
	var expiration time.Time
	extensions := e.Extensions()
	if deliveryTime, ok := extensions[xVanusDeliveryTime]; ok {
		if t, ok := deliveryTime.(ce.Timestamp); !ok {
			log.Error(ctx).
				Interface("time", deliveryTime).
				Msg("parse time failed")
			expiration = time.Now()
		} else {
			expiration = t.Time
		}
	} else {
		log.Error(ctx).Msg("xvanusdeliverytime not found, set to current time")
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
				log.Debug(ctx).Msg("context canceled at bucket update offset metadata")
				return
			case <-b.exitC:
				log.Debug(ctx).Msg("bucket exit at bucket update offset metadata")
				return
			case offset := <-offsetC:
				// wait for all goroutines to finish before updating offset metadata
				offset.wg.Wait()
				log.Debug(ctx).
					Str("eventbus", b.eventbus).
					Int64("update_to", offset.data).
					Msg("update offset metadata")
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
				log.Debug(ctx).
					Str("eventbus", b.getEventbus()).
					Msg("context canceled at bucket running")
				return
			case <-b.exitC:
				log.Debug(ctx).
					Str("eventbus", b.getEventbus()).
					Msg("bucket exit at bucket running")
				return
			default:
				// batch read
				events, err := b.getEvent(ctx, defaultNumberOfEventsRead)
				if err != nil {
					if !errors.Is(err, errors.ErrOffsetOnEnd) && !errors.Is(err, errors.ErrTryAgain) {
						log.Error(ctx).Err(err).
							Str("eventbus", b.getEventbus()).
							Msg("get event failed when bucket running")
					}
					time.Sleep(sleepDuration)
					break
				}
				if len(events) == 0 {
					time.Sleep(sleepDuration)
					log.Debug(ctx).
						Str("function", "run").
						Msg("no more message")
					break
				}
				// concurrent write
				numberOfEvents := int64(len(events))
				log.Debug(ctx).
					Str("eventbus", b.eventbus).
					Int64("offset", b.offset).
					Int64("layer", b.layer).
					Int64("number_of_events", numberOfEvents).
					Msg("got events when bucket running")

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
			log.Warn(ctx).
				Str("eventbus", b.eventbus).
				Str("event_id", e.ID()).
				Time("expiration", tm.getExpiration()).
				Msg("push event to distribution station failed, retry until it succeed")
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
			log.Warn(ctx).
				Str("eventbus", b.eventbus).
				Str("event_id", e.ID()).
				Time("expiration", tm.getExpiration()).
				Msg("push event failed, retry until it succeed")
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
	_, err := b.timingwheel.ctrl.EventbusService().CreateSystemEventbusIfNotExist(ctx, b.eventbus,
		"System Eventbus For Timing Service")
	if err != nil {
		log.Error(ctx).
			Str(log.KeyEventbusName, b.eventbus).
			Msg("failed to create timer eventbus")
		return err
	}
	log.Info(ctx).
		Str(log.KeyEventbusName, b.eventbus).
		Msg("success to create timer eventbus")
	return nil
}

func (b *bucket) connectEventbus(ctx context.Context) {
	b.eventbusWriter = b.client.Eventbus(ctx, api.WithName(b.eventbus)).Writer()
	b.eventbusReader = b.client.Eventbus(ctx, api.WithName(b.eventbus)).Reader()
}

func (b *bucket) putEvent(ctx context.Context, tm *timingMsg) (err error) {
	defer func() {
		if errOfPanic := recover(); errOfPanic != nil {
			log.Warn(ctx).Err(err).
				Msg("panic when put event")
			err = stderr.New("panic when put event")
		}
	}()
	if !b.isLeader() {
		return nil
	}
	_, err = api.AppendOne(ctx, b.eventbusWriter, tm.getEvent())
	if err != nil {
		log.Error(ctx).Err(err).
			Str("eventbus", b.eventbus).
			Time("expiration", tm.getExpiration()).
			Msg("append event to failed")
		return err
	}
	log.Debug(ctx).
		Str("eventbus", b.eventbus).
		Str("event_id", tm.event.ID()).
		Time("expiration", tm.getExpiration()).
		Msg("put event success")
	return err
}

func (b *bucket) getEvent(ctx context.Context, number int16) (events []*ce.Event, err error) {
	defer func() {
		if errOfPanic := recover(); errOfPanic != nil {
			log.Warn(ctx).Err(err).
				Msg("panic when get event")
			events = []*ce.Event{}
			err = errors.ErrOffsetOnEnd
		}
	}()
	if !b.isLeader() {
		// TODO(jiangkai): redesign here for reduce cpu overload, by jiangkai, 2022.09.16
		time.Sleep(time.Second)
		return []*ce.Event{}, errors.ErrOffsetOnEnd
	}
	ls, err := b.client.Eventbus(ctx, api.WithName(b.eventbus)).ListLog(ctx)
	if err != nil {
		return []*ce.Event{}, err
	}

	readPolicy := option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], b.offset))
	events, _, _, err = api.Read(ctx, b.eventbusReader, readPolicy, option.WithBatchSize(int(number)))
	if err != nil {
		if !errors.Is(err, errors.ErrOffsetOnEnd) &&
			!stderr.Is(ctx.Err(), context.Canceled) {
			log.Error(ctx).Err(err).Msg("read failed")
		}
		return nil, err
	}
	return events, err
}

func (b *bucket) updateOffsetMeta(ctx context.Context, offset int64) {
	if !b.isLeader() {
		return
	}
	key := fmt.Sprintf("%s/%s", metadata.OffsetKeyPrefixInKVStore, b.eventbus)
	offsetMeta := &metadata.OffsetMeta{
		Layer:    b.layer,
		Slot:     b.slot,
		Offset:   offset,
		Eventbus: b.eventbus,
	}
	data, _ := json.Marshal(offsetMeta)
	err := b.kvStore.Set(ctx, key, data)
	if err != nil {
		log.Warn(ctx).Err(err).
			Str("key", key).
			Str("eventbus", b.eventbus).
			Int64("slot", b.slot).
			Int64("layer", b.layer).
			Int64("offset", b.offset).
			Msg("set offset metadata to kvstore failed")
	}
}

func (b *bucket) existsOffsetMeta(ctx context.Context) (bool, error) {
	key := fmt.Sprintf("%s/%s", metadata.OffsetKeyPrefixInKVStore, b.eventbus)
	return b.kvStore.Exists(ctx, key)
}

func (b *bucket) getOffsetMeta(ctx context.Context) (int64, error) {
	if !b.isLeader() {
		return -1, nil
	}
	key := fmt.Sprintf("%s/%s", metadata.OffsetKeyPrefixInKVStore, b.eventbus)
	value, err := b.kvStore.Get(ctx, key)
	if err != nil {
		log.Warn(ctx).Err(err).
			Str("key", key).
			Str("eventbus", b.eventbus).
			Int64("slot", b.slot).
			Int64("layer", b.layer).
			Int64("offset", b.offset).
			Msg("get offset metadata from kvstore failed")
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
	key := fmt.Sprintf("%s/%s", metadata.OffsetKeyPrefixInKVStore, b.eventbus)
	err := b.kvStore.Delete(ctx, key)
	if err != nil {
		log.Warn(ctx).Err(err).Str("key", key).
			Msg("delete offset metadata to kvstore failed")
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
	// TODO(jiangkai): check for errors
	metaEventbus, _ := b.timingwheel.ctrl.EventbusService().GetSystemEventbusByName(ctx, b.eventbus)
	_ = b.timingwheel.ctrl.EventbusService().Delete(ctx, metaEventbus.Id)
	_ = b.deleteOffsetMeta(ctx)
}

func (b *bucket) wait(_ context.Context) {
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
