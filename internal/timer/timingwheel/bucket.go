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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	"k8s.io/apimachinery/pkg/util/wait"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	eb "github.com/linkall-labs/vanus/client"
	es "github.com/linkall-labs/vanus/client/pkg/errors"
	eventbus "github.com/linkall-labs/vanus/client/pkg/eventbus"
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

const (
	timerBuiltInEventbusReceivingStation    = "__Timer_RS"
	timerBuiltInEventbusDistributionStation = "__Timer_DS"
	timerBuiltInEventbus                    = "__Timer_%d_%d"
	xVanusEventbus                          = "xvanuseventbus"
	xVanusDeliveryTime                      = "xvanusdeliverytime"
)

var (
	openBusWriter      = eb.OpenBusWriter
	lookupReadableLogs = eb.LookupReadableLogs
	openLogReader      = eb.OpenLogReader
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
	return time.Now().After(tm.expiration)
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
	kvStore        kv.Client
	client         ctrlpb.EventBusControllerClient
	eventbusWriter eventbus.BusWriter
	eventlogReader eventlog.LogReader

	timingwheel *timingWheel
	element     *list.Element
}

func newBucket(tw *timingWheel, element *list.Element, tick time.Duration, ebName string, layer, slot int64) *bucket {
	return &bucket{
		config:      tw.config,
		tick:        tick,
		layer:       layer,
		slot:        slot,
		offset:      0,
		interval:    tick * time.Duration(tw.config.WheelSize),
		eventbus:    ebName,
		kvStore:     tw.kvStore,
		client:      tw.client,
		timingwheel: tw,
		element:     element,
	}
}

func (b *bucket) start(ctx context.Context) error {
	var err error
	if err = b.createEventbus(ctx); err != nil {
		return err
	}

	if err = b.connectEventbus(ctx); err != nil {
		return err
	}

	b.run(ctx)
	return nil
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
			default:
				// batch read
				events, err := b.getEvent(ctx, defaultNumberOfEventsRead)
				if err != nil {
					if !errors.Is(err, es.ErrOnEnd) && !errors.Is(err, es.ErrTryAgain) {
						log.Error(ctx, "get event failed when bucket running", map[string]interface{}{
							"eventbus":   b.getEventbus(),
							log.KeyError: err,
						})
					}
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
						tm := newTimingMsg(ctx, e)
						waitCtx, cancel := context.WithCancel(ctx)
						wait.Until(func() {
							if b.layer == 1 || tm.hasExpired() {
								if b.timingwheel.getDistributionStation().push(ctx, tm) == nil {
									cancel()
								} else {
									log.Warning(ctx, "push event to distribution station failed, retry until it succeed", map[string]interface{}{
										"eventbus": b.eventbus,
										"event":    e.String(),
									})
								}
							} else {
								if b.getTimingWheelElement().prev().push(ctx, tm, true) {
									cancel()
								} else {
									log.Warning(ctx, "push event to prev timingwheel failed, retry until it succeed", map[string]interface{}{
										"eventbus": b.eventbus,
										"event":    e.String(),
									})
								}
							}
						}, b.config.Tick/defaultCheckWaitingPeriodRatio, waitCtx.Done())
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

func (b *bucket) waitingForReady(ctx context.Context, events []*ce.Event) {
	tm := newTimingMsg(ctx, events[0])
	blockCtx, blockCancel := context.WithCancel(ctx)
	wait.Until(func() {
		if tm.hasExpired() {
			blockCancel()
		}
		if b.layer > 1 && b.isReadyToFlow(tm) {
			log.Debug(ctx, "the bucket is ready to flow", map[string]interface{}{
				"eventbus": b.eventbus,
				"offset":   b.offset,
				"layer":    b.layer,
			})
			blockCancel()
		}
	}, b.tick/defaultFrequentCheckWaitingPeriodRatio, blockCtx.Done())
}

func (b *bucket) isReadyToFlow(tm *timingMsg) bool {
	startTimeOfBucket := tm.getExpiration().UnixNano() - (tm.getExpiration().UnixNano() % b.tick.Nanoseconds())
	advanceTimeOfFlow := defaultNumberOfTickLoadsInAdvance * b.getTimingWheelElement().prev().tick
	return time.Now().UTC().Add(advanceTimeOfFlow).UnixNano() > startTimeOfBucket
}

func (b *bucket) push(ctx context.Context, tm *timingMsg) error {
	return b.putEvent(ctx, tm)
}

func (b *bucket) isExistEventbus(ctx context.Context) bool {
	_, err := b.client.GetEventBus(ctx, &meta.EventBus{Name: b.eventbus})
	return err == nil
}

func (b *bucket) createEventbus(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.isLeader() || b.isExistEventbus(ctx) {
		return nil
	}
	_, err := b.client.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name: b.eventbus,
	})
	if err != nil {
		log.Error(ctx, "create eventbus failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   b.eventbus,
		})
		return err
	}
	log.Info(ctx, "create eventbus success.", map[string]interface{}{
		"eventbus": b.eventbus,
	})
	return nil
}

func (b *bucket) connectEventbus(ctx context.Context) error {
	var (
		err error
		vrn string
	)

	vrn = fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", b.eventbus, strings.Join(b.config.CtrlEndpoints, ","))
	// new eventbus writer
	b.eventbusWriter, err = openBusWriter(ctx, vrn)
	if err != nil {
		log.Error(ctx, "open eventbus writer failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	ls, err := lookupReadableLogs(ctx, vrn)
	if err != nil {
		log.Error(ctx, "lookup readable logs failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	// new eventlog reader
	b.eventlogReader, err = openLogReader(ctx, ls[defaultIndexOfEventlogReader].VRN)
	if err != nil {
		log.Error(ctx, "open log reader failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	return nil
}

func (b *bucket) putEvent(ctx context.Context, tm *timingMsg) (err error) {
	defer func() {
		if errOfPanic := recover(); errOfPanic != nil {
			log.Warning(ctx, "panic when put event", map[string]interface{}{
				log.KeyError: errOfPanic,
			})
			err = errors.New("panic when put event")
		}
	}()
	if !b.isLeader() {
		return nil
	}
	_, err = b.eventbusWriter.Append(ctx, tm.getEvent())
	if err != nil {
		log.Error(ctx, "append event to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   b.eventbus,
			"expiration": tm.getExpiration().Format(time.RFC3339Nano),
		})
		return err
	}
	log.Debug(ctx, "put event success", map[string]interface{}{
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
			err = es.ErrOnEnd
		}
	}()
	if !b.isLeader() {
		// TODO(jiangkai): redesign here for reduce cpu overload, by jiangkai, 2022.09.16
		time.Sleep(time.Second)
		return []*ce.Event{}, es.ErrOnEnd
	}
	_, err = b.eventlogReader.Seek(ctx, b.offset, io.SeekStart)
	if err != nil {
		log.Error(ctx, "seek failed", map[string]interface{}{
			log.KeyError: err,
			"offset":     b.offset,
		})
		return nil, err
	}

	events, err = b.eventlogReader.Read(ctx, number)
	if err != nil {
		if !errors.Is(err, es.ErrOnEnd) && !errors.Is(ctx.Err(), context.Canceled) {
			log.Error(ctx, "read failed", map[string]interface{}{
				log.KeyError: err,
				"offset":     b.offset,
			})
		}
		return nil, err
	}

	log.Debug(ctx, "get event success", map[string]interface{}{
		"eventbus": b.eventbus,
		"offset":   b.offset,
		"number":   number,
	})
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
