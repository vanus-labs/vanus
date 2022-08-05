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
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

const (
	timerBuiltInEventbusReceivingStation    = "__Timer_RS"
	timerBuiltInEventbusDistributionStation = "__Timer_DS"
	timerBuiltInEventbus                    = "__Timer_%d_%d"
	xceVanusEventbus                        = "xvanuseventbus"
	xceVanusDeliveryTime                    = "xvanusdeliverytime"
)

var (
	lookupReadableLogs = eb.LookupReadableLogs
	openLogWriter      = eb.OpenLogWriter
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
	if _, ok := extensions[xceVanusDeliveryTime]; ok {
		expiration, err = types.ParseTime(extensions[xceVanusDeliveryTime].(string))
		if err != nil {
			log.Error(ctx, "parse time failed", map[string]interface{}{
				log.KeyError: err,
				"time":       extensions[xceVanusDeliveryTime].(string),
			})
			expiration = time.Now().UTC()
		}
	} else {
		log.Error(ctx, "xvanusdeliverytime not found, set to current time", nil)
		expiration = time.Now().UTC()
	}
	return &timingMsg{
		expiration: expiration.UTC(),
		event:      e,
	}
}

func (tm *timingMsg) isExpired(tick time.Duration) bool {
	return time.Now().UTC().Add(tick).After(tm.expiration)
}

func (tm *timingMsg) consume(ctx context.Context, endpoints []string) error {
	var (
		err            error
		vrn            string
		ebName         string
		eventlogWriter eventlog.LogWriter
	)
	err = tm.event.ExtensionAs(xceVanusEventbus, &ebName)
	if err != nil {
		log.Error(ctx, "get eventbus failed when consume", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	vrn = fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", ebName, strings.Join(endpoints, ","))
	ls, err := lookupReadableLogs(ctx, vrn)
	if err != nil {
		log.Error(ctx, "lookup readable logs failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	// new eventlog writer
	eventlogWriter, err = openLogWriter(ls[defaultIndexOfEventlogWriter].VRN)
	if err != nil {
		log.Error(ctx, "open log writer failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	offset, err := eventlogWriter.Append(ctx, tm.event)
	defer eventlogWriter.Close()
	if err != nil {
		log.Error(ctx, "consume event failed", map[string]interface{}{
			log.KeyError: err,
			"expiration": tm.expiration,
		})
		return err
	}
	log.Info(ctx, "consume event success", map[string]interface{}{
		"expiration": tm.expiration,
		"offset":     offset,
	})
	return nil
}

type bucket struct {
	config   *Config
	tick     time.Duration
	layer    int64
	slot     int64
	offset   int64
	eventbus string

	mu             sync.Mutex
	kvStore        kv.Client
	client         *ctrlClient
	eventlogWriter eventlog.LogWriter
	eventlogReader eventlog.LogReader
}

func newBucket(c *Config, store kv.Client, cli *ctrlClient,
	tick time.Duration, ebName string, layer, slot int64) *bucket {
	return &bucket{
		config:   c,
		tick:     tick,
		layer:    layer,
		slot:     slot,
		offset:   0,
		eventbus: ebName,
		kvStore:  store,
		client:   cli,
	}
}

// create buckets for each layer of time wheel.
func createBucketsForTimingWheel(c *Config, store kv.Client, cli *ctrlClient,
	tick time.Duration, layer int64) []*bucket {
	var (
		i       int64
		buckets []*bucket
	)

	buckets = make([]*bucket, c.WheelSize)
	for i = 0; i < c.WheelSize; i++ {
		ebName := fmt.Sprintf(timerBuiltInEventbus, layer, i)
		buckets[i] = newBucket(c, store, cli, tick, ebName, layer, i)
	}
	return buckets
}

func (b *bucket) start(ctx context.Context) error {
	var (
		err error
		vrn string
	)

	vrn = fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", b.eventbus, strings.Join(b.config.CtrlEndpoints, ","))
	ls, err := lookupReadableLogs(ctx, vrn)
	if err != nil {
		log.Error(ctx, "lookup readable logs failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	// new eventlog writer
	b.eventlogWriter, err = openLogWriter(ls[defaultIndexOfEventlogWriter].VRN)
	if err != nil {
		log.Error(ctx, "open log writer failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	// new eventlog reader
	b.eventlogReader, err = openLogReader(ls[defaultIndexOfEventlogReader].VRN)
	if err != nil {
		log.Error(ctx, "open log reader failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	return nil
}

func (b *bucket) add(ctx context.Context, tm *timingMsg, check bool) error {
	if check {
		// just for highest layer timingwheel
		err := b.createEventbus(ctx)
		if err != nil {
			log.Error(ctx, "bucket create eventbus failed", map[string]interface{}{
				log.KeyError: err,
				"eventbus":   b.eventbus,
			})
			return err
		}
		err = b.start(ctx)
		if err != nil {
			log.Error(ctx, "bucket start failed", map[string]interface{}{
				log.KeyError: err,
				"eventbus":   b.eventbus,
			})
			return err
		}
	}
	return b.putEvent(ctx, tm)
}

func (b *bucket) buildTimingMessageTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Warning(ctx, "context canceled at bucket distribute", map[string]interface{}{
				"eventbus": b.eventbus,
			})
			return
		default:
			events, err := b.getEvent(ctx, defaultNumberOfEventsRead)
			if err != nil {
				if !errors.Is(err, es.ErrOnEnd) && !errors.Is(err, es.ErrTryAgain) {
					log.Error(ctx, "get event failed when bucket distribute", map[string]interface{}{
						"eventbus":   b.eventbus,
						log.KeyError: err,
					})
				}
				return
			}

			tm := newTimingMsg(ctx, events[defaultIndexOfEventsRead])
			waitCtx, cancel := context.WithCancel(ctx)
			wait.Until(func() {
				if time.Now().UTC().After(tm.expiration) {
					cancel()
				}
			}, b.tick/defaultFlushWaitingPeriodRatio, waitCtx.Done())

			err = tm.consume(ctx, b.config.CtrlEndpoints)
			if err != nil {
				log.Error(ctx, "consume event failed", map[string]interface{}{
					"expiration": tm.expiration,
				})
				break
			}
			b.offset++
			if err = b.updateOffsetMeta(ctx); err != nil {
				log.Warning(ctx, "update offset metadata failed", map[string]interface{}{
					log.KeyError: err,
					"layer":      b.layer,
					"slot":       b.slot,
					"offset":     b.offset,
					"eventbus":   b.eventbus,
				})
			}
		}
	}
}

func (b *bucket) fetchEventFromOverflowWheelAdvance(ctx context.Context,
	reInsert func(context.Context, *timingMsg) bool) {
	for {
		select {
		case <-ctx.Done():
			log.Debug(ctx, "context canceled at bucket load", nil)
			return
		default:
			events, err := b.getEvent(ctx, defaultNumberOfEventsRead)
			if err != nil {
				if !errors.Is(err, es.ErrOnEnd) && !errors.Is(ctx.Err(), context.Canceled) {
					log.Error(ctx, "get event failed when bucket load", map[string]interface{}{
						"eventbus":   b.eventbus,
						"offset":     b.offset,
						log.KeyError: err,
					})
				}
				return
			}

			tm := newTimingMsg(ctx, events[defaultIndexOfEventsRead])
			log.Debug(ctx, "load event to next layer timingwheel", map[string]interface{}{
				"sourceEventbus": b.eventbus,
				"expiration":     tm.expiration,
			})
			tmCtx, cancel := context.WithCancel(ctx)
			wait.Until(func() {
				if tm.isExpired(b.tick) {
					cancel()
				}
			}, b.tick/defaultLoadWaitingPeriodRatio, tmCtx.Done())

			if reInsert(ctx, tm) {
				log.Info(ctx, "reinsert timing message success", map[string]interface{}{
					"sourceEventbus": b.eventbus,
					"expiration":     tm.expiration,
				})
				b.offset++
				if err = b.updateOffsetMeta(ctx); err != nil {
					log.Warning(ctx, "update offset metadata failed", map[string]interface{}{
						log.KeyError: err,
						"layer":      b.layer,
						"slot":       b.slot,
						"offset":     b.offset,
						"eventbus":   b.eventbus,
					})
				}
			}
		}
	}
}

func (b *bucket) isExistEventbus(ctx context.Context) bool {
	_, err := b.client.leaderClient.GetEventBus(ctx, &meta.EventBus{Name: b.eventbus})
	return err == nil
}

func (b *bucket) createEventbus(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isExistEventbus(ctx) {
		return nil
	}
	_, err := b.client.leaderClient.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
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

func (b *bucket) putEvent(ctx context.Context, tm *timingMsg) error {
	_, err := b.eventlogWriter.Append(ctx, tm.event)
	if err != nil {
		log.Error(ctx, "append event to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   b.eventbus,
			"expiration": tm.expiration,
		})
		return err
	}
	log.Debug(ctx, "put event success", map[string]interface{}{
		"eventbus":  b.eventbus,
		"eventTime": tm.expiration,
	})
	return nil
}

func (b *bucket) getEvent(ctx context.Context, number int16) ([]*ce.Event, error) {
	var err error
	_, err = b.eventlogReader.Seek(ctx, b.offset, io.SeekStart)
	if err != nil {
		log.Error(ctx, "seek failed", map[string]interface{}{
			log.KeyError: err,
			"offset":     b.offset,
		})
		return nil, err
	}

	events, err := b.eventlogReader.Read(ctx, number)
	if err != nil {
		if !errors.Is(err, es.ErrOnEnd) && !errors.Is(ctx.Err(), context.Canceled) {
			log.Error(ctx, "Read failed", map[string]interface{}{
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
	return events, nil
}

func (b *bucket) updateOffsetMeta(ctx context.Context) error {
	key := fmt.Sprintf("%s/offset/%s", metadata.MetadataKeyPrefixInKVStore, b.eventbus)
	offsetMeta := &metadata.OffsetMeta{
		Layer:    b.layer,
		Slot:     b.slot,
		Offset:   b.offset,
		Eventbus: b.eventbus,
	}
	data, _ := json.Marshal(offsetMeta)
	err := b.kvStore.Set(ctx, key, data)
	if err != nil {
		log.Warning(ctx, "set offset metadata to kvstore failed", map[string]interface{}{
			log.KeyError: err,
			"key":        key,
			"data":       data,
		})
		return err
	}
	return nil
}
