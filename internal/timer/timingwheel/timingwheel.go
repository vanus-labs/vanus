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

	ce "github.com/cloudevents/sdk-go/v2"
	errcli "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/pkg/controller"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// check waiting period every 1/defaultCheckWaitingPeriodRatio tick time by default.
	defaultCheckWaitingPeriodRatio = 10

	// frequent check waiting period every 1/defaultFrequentCheckWaitingPeriodRatio tick time by default.
	defaultFrequentCheckWaitingPeriodRatio = 100

	// number of tick loads in advance by default.
	defaultNumberOfTickLoadsInAdvance = 1

	// number of events read each time by default.
	defaultNumberOfEventsRead = 10

	// the max number of workers by default.
	defaultMaxNumberOfWorkers = 1000

	// index of eventlog reader by default.
	defaultIndexOfEventlogReader = 0

	heartbeatInterval = 5 * time.Second
)

var (
	newEtcdClientV3 = etcd.NewEtcdClientV3
)

type Manager interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Push(ctx context.Context, e *ce.Event) bool
	SetLeader(isleader bool)
	IsLeader() bool
	IsDeployed(ctx context.Context) bool
	Recover(ctx context.Context) error
	StopNotify() <-chan struct{}
	Stop(ctx context.Context)
}

// timingWheel timewheel contains multiple layers.
type timingWheel struct {
	config  *Config
	kvStore kv.Client
	client  ctrlpb.EventBusControllerClient
	twList  *list.List // element: *timingWheelElement

	receivingStation    *bucket
	distributionStation *bucket

	leader bool
	exitC  chan struct{}
	wg     sync.WaitGroup
}

func NewTimingWheel(c *Config) Manager {
	store, err := newEtcdClientV3(c.EtcdEndpoints, c.KeyPrefix)
	if err != nil {
		log.Error(context.Background(), "new etcd client v3 failed", map[string]interface{}{
			log.KeyError: err,
			"endpoints":  c.EtcdEndpoints,
			"key_prefix": c.KeyPrefix,
		})
		panic("new etcd client failed")
	}

	log.Info(context.Background(), "new timingwheel manager", map[string]interface{}{
		"tick":           c.Tick,
		"layers":         c.Layers,
		"wheel_size":     c.WheelSize,
		"key_prefix":     c.KeyPrefix,
		"etcd_endpoints": c.EtcdEndpoints,
		"ctrl_endpoints": c.CtrlEndpoints,
	})
	metrics.TimingWheelTickGauge.Set(float64(c.Tick))
	metrics.TimingWheelSizeGauge.Set(float64(c.WheelSize))
	metrics.TimingWheelLayersGauge.Set(float64(c.Layers))
	return &timingWheel{
		config:  c,
		kvStore: store,
		client:  controller.NewEventbusClient(c.CtrlEndpoints, insecure.NewCredentials()),
		twList:  list.New(),
		leader:  false,
		exitC:   make(chan struct{}),
	}
}

// Init init the current timing wheel.
func (tw *timingWheel) Init(ctx context.Context) error {
	log.Info(ctx, "init timingwheel", nil)
	// Init Hierarchical Timing Wheels.
	for layer := int64(1); layer <= tw.config.Layers+1; layer++ {
		tick := exponent(tw.config.Tick, tw.config.WheelSize, layer-1)
		twe := newTimingWheelElement(tw, tick, layer)
		twe.setElement(tw.twList.PushBack(twe))
		if layer <= tw.config.Layers {
			buckets := make(map[int64]*bucket, tw.config.WheelSize+defaultNumberOfTickLoadsInAdvance)
			for i := int64(0); i < tw.config.WheelSize+defaultNumberOfTickLoadsInAdvance; i++ {
				ebName := fmt.Sprintf(timerBuiltInEventbus, layer, i)
				buckets[i] = newBucket(tw, twe.element, tick, ebName, layer, i)
			}
			twe.buckets = buckets
		}
	}
	tw.receivingStation = newBucket(tw, nil, 0, timerBuiltInEventbusReceivingStation, 0, 0)
	tw.distributionStation = newBucket(tw, nil, 0, timerBuiltInEventbusDistributionStation, 0, 0)

	return nil
}

// Start starts the current timing wheel.
func (tw *timingWheel) Start(ctx context.Context) error {
	var err error
	log.Info(ctx, "start timingwheel", map[string]interface{}{
		"leader": tw.leader,
	})

	// here is to wait for the leader to complete the creation of all eventbus
	waitCtx, cancel := context.WithCancel(ctx)
	wait.Until(func() {
		if tw.IsLeader() || tw.IsDeployed(ctx) {
			cancel()
		}
		log.Info(ctx, "wait for the leader to be ready", nil)
	}, time.Second, waitCtx.Done())

	// start distribution station for scheduled events distributing
	if err = tw.startDistributionStation(ctx); err != nil {
		return err
	}

	// start all bucket of each layer
	for e := tw.twList.Front(); e != nil; e = e.Next() {
		for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
			if err = bucket.start(ctx); err != nil {
				log.Error(ctx, "start bucket failed", map[string]interface{}{
					log.KeyError: err,
					"eventbus":   bucket.getEventbus(),
				})
				return err
			}
		}
	}

	// start receving station for scheduled events receiving
	if err = tw.startReceivingStation(ctx); err != nil {
		return err
	}

	// start controller client heartbeat
	tw.startHeartBeat(ctx)

	return nil
}

func (tw *timingWheel) StopNotify() <-chan struct{} {
	return tw.exitC
}

// Stop stops the current timing wheel.
func (tw *timingWheel) Stop(ctx context.Context) {
	log.Info(ctx, "stop timingwheel", nil)
	// wait for all goroutine to end
	for e := tw.twList.Front(); e != nil; e = e.Next() {
		for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
			bucket.wait(ctx)
		}
		e.Value.(*timingWheelElement).wait(ctx)
	}
	tw.receivingStation.wait(ctx)
	tw.distributionStation.wait(ctx)
	close(tw.exitC)
	tw.wg.Wait()
	if closer, ok := tw.client.(io.Closer); ok {
		_ = closer.Close()
	}
}

func (tw *timingWheel) SetLeader(isLeader bool) {
	tw.leader = isLeader
}

func (tw *timingWheel) IsLeader() bool {
	return tw.leader
}

func (tw *timingWheel) IsDeployed(ctx context.Context) bool {
	return tw.receivingStation.isExistEventbus(ctx) && tw.distributionStation.isExistEventbus(ctx)
}

func (tw *timingWheel) Recover(ctx context.Context) error {
	offsetPath := fmt.Sprintf("%s/offset", metadata.MetadataKeyPrefixInKVStore)
	offsetPairs, err := tw.kvStore.List(ctx, offsetPath)
	if err != nil {
		return err
	}
	// no offset metadata, no recovery required
	if len(offsetPairs) == 0 {
		return nil
	}
	offsetMetaMap := make(map[string]*metadata.OffsetMeta, tw.config.Layers+1)
	for _, v := range offsetPairs {
		md := &metadata.OffsetMeta{}
		_ = json.Unmarshal(v.Value, md)
		if md.Layer > tw.config.Layers &&
			tw.twList.Back().Value.(*timingWheelElement).makeSureBucketExist(ctx, md.Slot) != nil {
			return err
		}
		offsetMetaMap[md.Eventbus] = md
	}

	for e := tw.twList.Front(); e != nil; e = e.Next() {
		for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
			if v, ok := offsetMetaMap[bucket.getEventbus()]; ok {
				log.Info(ctx, "recover offset metadata", map[string]interface{}{
					"layer":    v.Layer,
					"slot":     v.Slot,
					"offset":   v.Offset,
					"eventbus": v.Eventbus,
				})
				bucket.offset = v.Offset
			}
		}
	}

	log.Info(ctx, "recover receiving station metadata", map[string]interface{}{
		"offset":   offsetMetaMap[timerBuiltInEventbusReceivingStation].Offset,
		"eventbus": tw.receivingStation.getEventbus(),
	})
	tw.receivingStation.offset = offsetMetaMap[timerBuiltInEventbusReceivingStation].Offset
	log.Info(ctx, "recover distribution station metadata", map[string]interface{}{
		"offset":   offsetMetaMap[timerBuiltInEventbusDistributionStation].Offset,
		"eventbus": tw.distributionStation.getEventbus(),
	})
	tw.distributionStation.offset = offsetMetaMap[timerBuiltInEventbusDistributionStation].Offset

	return nil
}

// Push the scheduled event to the timingwheel.
func (tw *timingWheel) Push(ctx context.Context, e *ce.Event) bool {
	tm := newTimingMsg(ctx, e)
	log.Info(ctx, "push event to timingwheel", map[string]interface{}{
		"event":      e.String(),
		"expiration": tm.getExpiration().Format(time.RFC3339Nano),
	})

	if tm.hasExpired() {
		// Already expired
		return tw.getDistributionStation().push(ctx, tm) == nil
	}

	return tw.twList.Front().Value.(*timingWheelElement).push(ctx, tm, false)
}

func (tw *timingWheel) getReceivingStation() *bucket {
	return tw.receivingStation
}

func (tw *timingWheel) getDistributionStation() *bucket {
	return tw.distributionStation
}

func (tw *timingWheel) startHeartBeat(ctx context.Context) {
	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at timingwheel element heartbeat", nil)
				return
			case <-ticker.C:
				// TODO redesign here, by wenfeng, 2022.09.05
				// err := tw.client.Heartbeat(ctx)
				// if err != nil {
				// 	log.Warning(ctx, "heartbeat failed, connection lost. try to reconnecting", map[string]interface{}{
				// 		log.KeyError: err,
				// 	})
				// }
			}
		}
	}()
}

func (tw *timingWheel) startReceivingStation(ctx context.Context) error {
	var err error
	if err = tw.getReceivingStation().createEventbus(ctx); err != nil {
		return err
	}

	if err = tw.getReceivingStation().connectEventbus(ctx); err != nil {
		return err
	}

	tw.runReceivingStation(ctx)
	return nil
}

// runReceivingStation as the unified entrance of scheduled events and pushed to the timingwheel.
func (tw *timingWheel) runReceivingStation(ctx context.Context) {
	offsetC := make(chan waitGroup, defaultMaxNumberOfWorkers)
	tw.wg.Add(1)
	// update offset asynchronously
	go func() {
		defer tw.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at receiving station update offset metadata", nil)
				return
			case offset := <-offsetC:
				// wait for all goroutines to finish before updating offset metadata
				offset.wg.Wait()
				log.Debug(ctx, "update offset metadata", map[string]interface{}{
					"eventbus":  tw.receivingStation.getEventbus(),
					"update_to": offset.data,
				})
				tw.receivingStation.updateOffsetMeta(ctx, offset.data)
			}
		}
	}()

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		// limit the number of goroutines to no more than defaultMaxNumberOfWorkers
		glimitC := make(chan struct{}, defaultMaxNumberOfWorkers)
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at receiving station running", nil)
				return
			default:
				// batch read
				events, err := tw.receivingStation.getEvent(ctx, defaultNumberOfEventsRead)
				if err != nil {
					if !errors.Is(err, errcli.ErrOnEnd) {
						log.Error(ctx, "get event failed when receiving station running", map[string]interface{}{
							log.KeyError: err,
							"eventbus":   tw.receivingStation.getEventbus(),
						})
					}
					break
				}

				// concurrent write
				numberOfEvents := int64(len(events))
				log.Debug(ctx, "got events when receiving station running", map[string]interface{}{
					"eventbus":         tw.receivingStation.getEventbus(),
					"offset":           tw.receivingStation.getOffset(),
					"number_of_events": numberOfEvents,
				})

				wg := sync.WaitGroup{}
				for _, event := range events {
					wg.Add(1)
					glimitC <- struct{}{}
					go func(ctx context.Context, e *ce.Event) {
						defer wg.Done()
						waitCtx, cancel := context.WithCancel(ctx)
						wait.Until(func() {
							if tw.Push(ctx, e) {
								cancel()
							} else {
								log.Warning(ctx, "push event to timingwheel failed, retry until it succeed", map[string]interface{}{
									"eventbus": tw.receivingStation.getEventbus(),
									"event":    e.String(),
								})
							}
						}, tw.config.Tick/defaultCheckWaitingPeriodRatio, waitCtx.Done())
						<-glimitC
					}(ctx, event)
				}
				// asynchronously update offset after the same batch of events are successfully written
				offsetC <- waitGroup{
					wg:   &wg,
					data: tw.receivingStation.getOffset() + numberOfEvents,
				}
				tw.receivingStation.incOffset(numberOfEvents)
			}
		}
	}()
}

func (tw *timingWheel) startDistributionStation(ctx context.Context) error {
	var err error
	if err = tw.getDistributionStation().createEventbus(ctx); err != nil {
		return err
	}

	if err = tw.getDistributionStation().connectEventbus(ctx); err != nil {
		return err
	}

	tw.runDistributionStation(ctx)
	return nil
}

// runDistributionStation as the unified exit of scheduled events and popped to the timingwheel.
func (tw *timingWheel) runDistributionStation(ctx context.Context) {
	offsetC := make(chan waitGroup, defaultMaxNumberOfWorkers)
	tw.wg.Add(1)
	// update offset asynchronously
	go func() {
		defer tw.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at distribution station update offset metadata", nil)
				return
			case offset := <-offsetC:
				// wait for all goroutines to finish before updating offset metadata
				offset.wg.Wait()
				log.Debug(ctx, "update offset metadata", map[string]interface{}{
					"eventbus":  tw.distributionStation.getEventbus(),
					"update_to": offset.data,
				})
				tw.distributionStation.updateOffsetMeta(ctx, offset.data)
			}
		}
	}()

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		// limit the number of goroutines to no more than defaultMaxNumberOfWorkers
		glimitC := make(chan struct{}, defaultMaxNumberOfWorkers)
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at distribution station running", nil)
				return
			default:
				// batch read
				events, err := tw.distributionStation.getEvent(ctx, defaultNumberOfEventsRead)
				if err != nil {
					if !errors.Is(err, errcli.ErrOnEnd) {
						log.Error(ctx, "get event failed when distribution station running", map[string]interface{}{
							log.KeyError: err,
							"eventbus":   tw.distributionStation.getEventbus(),
						})
					}
					break
				}
				// concurrent write
				numberOfEvents := int64(len(events))
				log.Debug(ctx, "got events when distribution station running", map[string]interface{}{
					"eventbus":         tw.distributionStation.getEventbus(),
					"offset":           tw.distributionStation.getOffset(),
					"number_of_events": numberOfEvents,
				})

				wg := sync.WaitGroup{}
				for _, event := range events {
					wg.Add(1)
					glimitC <- struct{}{}
					go func(ctx context.Context, e *ce.Event) {
						defer wg.Done()
						waitCtx, cancel := context.WithCancel(ctx)
						wait.Until(func() {
							if err = tw.deliver(ctx, e); err == nil {
								cancel()
							} else {
								log.Warning(ctx, "deliver event failed, retry until it succeed", map[string]interface{}{
									"eventbus": tw.distributionStation.getEventbus(),
									"event":    e.String(),
								})
							}
						}, tw.config.Tick/defaultCheckWaitingPeriodRatio, waitCtx.Done())
						<-glimitC
					}(ctx, event)
				}
				// asynchronously update offset after the same batch of events are successfully written
				offsetC <- waitGroup{
					wg:   &wg,
					data: tw.distributionStation.getOffset() + numberOfEvents,
				}
				tw.distributionStation.incOffset(numberOfEvents)
			}
		}
	}()
}

func (tw *timingWheel) deliver(ctx context.Context, e *ce.Event) error {
	var (
		err    error
		ebName string
	)

	err = e.ExtensionAs(xVanusEventbus, &ebName)
	if err != nil {
		log.Error(ctx, "get eventbus failed when delivering", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	vrn := fmt.Sprintf("vanus:///eventbus/%s?controllers=%s", ebName, strings.Join(tw.config.CtrlEndpoints, ","))
	eventbusWriter, err := openBusWriter(ctx, vrn)
	if err != nil {
		log.Error(ctx, "open eventbus writer failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	defer eventbusWriter.Close(ctx)
	_, err = eventbusWriter.Append(ctx, e)
	if err != nil {
		if errors.Is(err, errcli.ErrNotFound) {
			log.Warning(ctx, "eventbus not found, discard this event", map[string]interface{}{
				log.KeyError: err,
				"eventbus":   ebName,
				"event":      e.String(),
			})
			return nil
		}
		log.Error(ctx, "append failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   ebName,
		})
		return err
	}
	log.Debug(ctx, "event delivered", map[string]interface{}{
		"delivery-time": e.Extensions()[xVanusDeliveryTime],
		"eventbus":      e.Extensions()[xVanusEventbus],
	})
	return nil
}

// timingWheelElement timingwheelelement has N number of buckets, every bucket is an eventbus.
type timingWheelElement struct {
	config   *Config
	kvStore  kv.Client
	client   ctrlpb.EventBusControllerClient
	tick     time.Duration
	layer    int64
	interval time.Duration
	buckets  map[int64]*bucket

	exitC chan struct{}
	mu    sync.Mutex
	wg    sync.WaitGroup

	timingwheel *timingWheel
	element     *list.Element
}

// newTimingWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimingWheelElement(tw *timingWheel, tick time.Duration, layer int64) *timingWheelElement {
	if tick <= 0 {
		panic(errors.New("tick must be greater than or equal to 1s"))
	}

	return &timingWheelElement{
		config:      tw.config,
		kvStore:     tw.kvStore,
		client:      tw.client,
		tick:        tick,
		layer:       layer,
		interval:    tick * time.Duration(tw.config.WheelSize),
		exitC:       make(chan struct{}),
		timingwheel: tw,
	}
}

func (twe *timingWheelElement) push(ctx context.Context, tm *timingMsg, isFlowing bool) bool {
	if isFlowing || twe.allowPush(tm) || twe.layer > twe.config.Layers {
		index := twe.calculateIndex(tm, isFlowing)
		// Put it into its own bucket
		if twe.layer > twe.config.Layers && twe.makeSureBucketExist(ctx, index) != nil {
			log.Error(ctx, "push timing message failed because bucket not exist", map[string]interface{}{
				"eventbus":   twe.buckets[index].getEventbus(),
				"expiration": tm.getExpiration().Format(time.RFC3339Nano),
			})
			return false
		}
		if err := twe.buckets[index].push(ctx, tm); err != nil {
			log.Error(ctx, "push timing message failed", map[string]interface{}{
				"eventbus":   twe.buckets[index].getEventbus(),
				"expiration": tm.getExpiration().Format(time.RFC3339Nano),
			})
			return false
		}
		log.Debug(ctx, "push timing message success", map[string]interface{}{
			"eventbus":   twe.buckets[index].getEventbus(),
			"expiration": tm.getExpiration().Format(time.RFC3339Nano),
		})
		return true
	}
	// Out of the interval. Put it into the overflow wheel
	return twe.next().push(ctx, tm, false)
}

func (twe *timingWheelElement) allowPush(tm *timingMsg) bool {
	now := time.Now()
	timeOfBufferBoundaryLine := now.UnixNano() - (now.UnixNano() % twe.tick.Nanoseconds()) + twe.interval.Nanoseconds()
	return tm.getExpiration().UnixNano() < timeOfBufferBoundaryLine
}

func (twe *timingWheelElement) calculateIndex(tm *timingMsg, isFlowing bool) int64 {
	if isFlowing {
		// the timing message comes from the timingwheel of the upper layer
		startTimeOfBucket := tm.getExpiration().UnixNano() - (tm.getExpiration().UnixNano() % twe.interval.Nanoseconds())
		timeOfEarlyFlow := defaultNumberOfTickLoadsInAdvance * twe.tick.Nanoseconds()
		timeOfBufferBoundaryLine := startTimeOfBucket - timeOfEarlyFlow + twe.interval.Nanoseconds()
		if tm.getExpiration().UnixNano() >= timeOfBufferBoundaryLine {
			// Put it into its buffer bucket
			return (tm.getExpiration().UnixNano()-timeOfBufferBoundaryLine)/twe.tick.Nanoseconds() + twe.config.WheelSize
		}
	}
	// Put it into its own bucket
	if twe.layer > twe.config.Layers {
		return tm.getExpiration().UnixNano() / twe.tick.Nanoseconds()
	}
	return tm.getExpiration().UnixNano() % twe.interval.Nanoseconds() / twe.tick.Nanoseconds()
}

func (twe *timingWheelElement) makeSureBucketExist(ctx context.Context, index int64) error {
	twe.mu.Lock()
	defer twe.mu.Unlock()
	if _, ok := twe.buckets[index]; ok {
		return nil
	}
	ebName := fmt.Sprintf(timerBuiltInEventbus, twe.layer, index)
	if twe.buckets == nil {
		twe.buckets = make(map[int64]*bucket)
	}
	twe.buckets[index] = newBucket(twe.timingwheel, twe.element, twe.tick, ebName, twe.layer, index)
	if err := twe.buckets[index].start(ctx); err != nil {
		log.Error(ctx, "start bucket failed when makesure bucket exist", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   twe.buckets[index].getEventbus(),
		})
		return err
	}
	return nil
}

func (twe *timingWheelElement) wait(ctx context.Context) {
	twe.wg.Wait()
}

func (twe *timingWheelElement) getBuckets() map[int64]*bucket {
	return twe.buckets
}

func (twe *timingWheelElement) setElement(element *list.Element) {
	twe.element = element
}

func (twe *timingWheelElement) prev() *timingWheelElement {
	return twe.element.Prev().Value.(*timingWheelElement)
}

func (twe *timingWheelElement) next() *timingWheelElement {
	return twe.element.Next().Value.(*timingWheelElement)
}
