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
	"sync"
	"time"

	es "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	timererrors "github.com/linkall-labs/vanus/internal/timer/errors"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"k8s.io/apimachinery/pkg/util/wait"

	ce "github.com/cloudevents/sdk-go/v2"
)

const (
	// the consumer routine triggers the flush operation every 1/defaultFlushPeriodRatio tick time by default.
	defaultFlushPeriodRatio = 10

	// flush routine waiting period every 1/defaultFlushPeriodRatio tick time by default.
	defaultFlushWaitingPeriodRatio = 100

	// load routine waiting period every 1/defaultLoadWaitingPeriodRatio tick time by default.
	defaultLoadCheckWaitingPeriodRatio = 10

	// load routine trigger period every 1/defaultLoadTriggerPeriodRatio tick time by default.
	defaultLoadTriggerPeriodRatio = 10

	// load routine waiting period every 1/defaultLoadWaitingPeriodRatio tick time by default.
	defaultLoadWaitingPeriodRatio = 100

	// fetch event from overflowwheel advance number of tick times by default.
	defaultFetchEventFromOverflowWheelEndPointer = 2

	// number of events read each time by default.
	defaultNumberOfEventsRead = 1

	// index of events read each time by default.
	defaultIndexOfEventsRead = 0

	// index of eventlog reader by default.
	defaultIndexOfEventlogReader = 0

	// index of eventlog writer by default.
	defaultIndexOfEventlogWriter = 0

	heartbeatInterval = 5 * time.Second
)

var (
	newEtcdClientV3 = etcd.NewEtcdClientV3
)

type Manager interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	AddEvent(ctx context.Context, e *ce.Event) bool
	SetLeader(isleader bool)
	IsLeader() bool
	IsDeployed(ctx context.Context) bool
	RecoverForFailover(ctx context.Context) error
	StopNotify() <-chan struct{}
	Stop(ctx context.Context)
}

// timingWheel timewheel contains multiple layers.
type timingWheel struct {
	config  *Config
	kvStore kv.Client
	client  *ctrlClient
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
			"endpoints":  c.EtcdEndpoints,
			"keyprefix":  c.KeyPrefix,
			log.KeyError: err,
		})
		panic("new etcd client failed")
	}

	client := NewClient(c.CtrlEndpoints)

	log.Info(context.Background(), "new timingwheel manager", map[string]interface{}{
		"tick":          c.Tick,
		"layers":        c.Layers,
		"wheelSize":     c.WheelSize,
		"keyPrefix":     c.KeyPrefix,
		"startTime":     c.StartTime,
		"etcdEndpoints": c.EtcdEndpoints,
		"ctrlEndpoints": c.CtrlEndpoints,
	})
	metrics.TimingWheelTickGauge.Set(float64(c.Tick))
	metrics.TimingWheelSizeGauge.Set(float64(c.WheelSize))
	metrics.TimingWheelLayersGauge.Set(float64(c.Layers))
	return &timingWheel{
		config:  c,
		kvStore: store,
		client:  client,
		leader:  false,
		exitC:   make(chan struct{}),
	}
}

// Init init the current timing wheel.
func (tw *timingWheel) Init(ctx context.Context) error {
	log.Info(ctx, "init timingwheel", nil)
	l := list.New()
	// Init Hierarchical Timing Wheels.
	for layer := int64(1); layer <= tw.config.Layers+1; layer++ {
		tick := exponent(tw.config.Tick, tw.config.WheelSize, layer-1)
		add(l, newTimingWheelElement(tw.config, tw.kvStore, tw.client, tick, layer))
	}
	tw.twList = l
	tw.receivingStation = newBucket(tw.config, tw.kvStore, tw.client, 0, timerBuiltInEventbusReceivingStation, 0, 0)
	tw.distributionStation = newBucket(tw.config, tw.kvStore, tw.client, 0, timerBuiltInEventbusDistributionStation, 0, 0)

	// makesure controller client
	if tw.client.makeSureClient(ctx, true) == nil {
		return timererrors.ErrNoControllerLeader
	}
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

	// create eventbus and start of each layer bucket
	for e := tw.twList.Front(); e != nil; {
		for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
			if tw.IsLeader() {
				err = bucket.createEventbus(ctx)
				if err != nil {
					log.Error(ctx, "bucket create eventbus failed", map[string]interface{}{
						log.KeyError: err,
						"eventbus":   bucket.eventbus,
					})
					return err
				}
			}
			err = bucket.start(ctx)
			if err != nil {
				log.Error(ctx, "bucket start failed", map[string]interface{}{
					log.KeyError: err,
					"eventbus":   bucket.eventbus,
				})
				return err
			}
		}
		next := e.Next()
		e = next
	}

	// start receving station for scheduled events receiving
	if err = tw.startReceivingStation(ctx); err != nil {
		return err
	}

	// start distribution station for scheduled events distributing
	if err = tw.startDistributionStation(ctx); err != nil {
		return err
	}

	// start the timingwheel of each layer
	for e := tw.twList.Front(); e != nil; {
		e.Value.(*timingWheelElement).start(ctx)
		next := e.Next()
		e = next
	}

	// start ticking of pointer
	tw.startTickingOfPointer(ctx)

	// start routine for scheduled event distributer
	tw.twList.Front().Value.(*timingWheelElement).startScheduledEventDistributer(ctx)

	// start routine for scheduled event dispatcher
	tw.startScheduledEventDispatcher(ctx)

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
	close(tw.exitC)
	tw.wg.Wait()
}

func (tw *timingWheel) SetLeader(isLeader bool) {
	for e := tw.twList.Front(); e != nil; {
		e.Value.(*timingWheelElement).setLeader(isLeader)
		next := e.Next()
		e = next
	}
	tw.leader = isLeader
}

func (tw *timingWheel) IsLeader() bool {
	return tw.leader
}

func (tw *timingWheel) IsDeployed(ctx context.Context) bool {
	return tw.receivingStation.start(ctx) == nil && tw.distributionStation.start(ctx) == nil
}

func (tw *timingWheel) RecoverForFailover(ctx context.Context) error {
	pointerPath := fmt.Sprintf("%s/pointer", metadata.MetadataKeyPrefixInKVStore)
	pointerPairs, err := tw.kvStore.List(ctx, pointerPath)
	if err != nil {
		return err
	}
	pointerMetaMap := make(map[int64]int64, tw.config.Layers+1)
	for _, v := range pointerPairs {
		md := &metadata.PointerMeta{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		log.Info(ctx, "recover pointer metadata", map[string]interface{}{
			"layer":   md.Layer,
			"pointer": md.Pointer,
		})
		pointerMetaMap[md.Layer] = md.Pointer
	}

	offsetPath := fmt.Sprintf("%s/offset", metadata.MetadataKeyPrefixInKVStore)
	offsetPairs, err := tw.kvStore.List(ctx, offsetPath)
	if err != nil {
		return err
	}
	offsetMetaMap := make(map[string]*metadata.OffsetMeta, tw.config.Layers+1)
	for _, v := range offsetPairs {
		md := &metadata.OffsetMeta{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		if md.Layer > tw.config.Layers {
			if cap(tw.twList.Back().Value.(*timingWheelElement).getBuckets()) < int(md.Slot+1) {
				tw.twList.Back().Value.(*timingWheelElement).resetBucketsCapacity(md.Slot + 1)
			}
			if tw.twList.Back().Value.(*timingWheelElement).getBucket(md.Slot) == nil {
				ebName := fmt.Sprintf(timerBuiltInEventbus, tw.twList.Back().Value.(*timingWheelElement).getLayer(), md.Slot)
				newbucket := newBucket(tw.config, tw.kvStore, tw.client, tw.twList.Back().Value.(*timingWheelElement).getTick(),
					ebName, tw.twList.Back().Value.(*timingWheelElement).getLayer(), md.Slot)
				tw.twList.Back().Value.(*timingWheelElement).setBuckets(md.Slot, newbucket)
				if err = tw.twList.Back().Value.(*timingWheelElement).getBucket(md.Slot).start(ctx); err != nil {
					return err
				}
			}
		}
		offsetMetaMap[md.Eventbus] = md
	}

	for e := tw.twList.Front(); e != nil; {
		e.Value.(*timingWheelElement).setPointer(pointerMetaMap[e.Value.(*timingWheelElement).getLayer()])
		for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
			if v, ok := offsetMetaMap[bucket.eventbus]; ok {
				log.Info(ctx, "recover offset metadata", map[string]interface{}{
					"layer":    v.Layer,
					"slot":     v.Slot,
					"offset":   v.Offset,
					"eventbus": v.Eventbus,
				})
				bucket.offset = v.Offset
			}
		}
		next := e.Next()
		e = next
	}

	log.Info(ctx, "recover receiving station metadata", map[string]interface{}{
		"offset":   offsetMetaMap[timerBuiltInEventbusReceivingStation].Offset,
		"eventbus": tw.receivingStation.eventbus,
	})
	tw.receivingStation.offset = offsetMetaMap[timerBuiltInEventbusReceivingStation].Offset
	log.Info(ctx, "recover distribution station metadata", map[string]interface{}{
		"offset":   offsetMetaMap[timerBuiltInEventbusDistributionStation].Offset,
		"eventbus": tw.distributionStation.eventbus,
	})
	tw.distributionStation.offset = offsetMetaMap[timerBuiltInEventbusDistributionStation].Offset

	return nil
}

func (tw *timingWheel) AddEvent(ctx context.Context, e *ce.Event) bool {
	tm := newTimingMsg(ctx, e)
	log.Info(ctx, "add event to timingwheel", map[string]interface{}{
		"eventID":        e.ID(),
		"expirationTime": tm.expiration,
	})
	return tw.twList.Front().Value.(*timingWheelElement).addEvent(ctx, tm)
}

func (tw *timingWheel) startReceivingStation(ctx context.Context) error {
	var err error
	if tw.IsLeader() {
		if err = tw.receivingStation.createEventbus(ctx); err != nil {
			return err
		}
		if err = tw.receivingStation.updateOffsetMeta(ctx); err != nil {
			log.Warning(ctx, "update receiving station offset metadata failed", map[string]interface{}{
				log.KeyError: err,
				"offset":     tw.receivingStation.offset,
				"eventbus":   tw.receivingStation.eventbus,
			})
			return err
		}
	}
	err = tw.receivingStation.start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (tw *timingWheel) startDistributionStation(ctx context.Context) error {
	var err error
	if tw.IsLeader() {
		if err = tw.distributionStation.createEventbus(ctx); err != nil {
			return err
		}
		if err = tw.distributionStation.updateOffsetMeta(ctx); err != nil {
			log.Warning(ctx, "update distribution station offset metadata failed", map[string]interface{}{
				log.KeyError: err,
				"offset":     tw.distributionStation.offset,
				"eventbus":   tw.distributionStation.eventbus,
			})
			return err
		}
	}
	err = tw.distributionStation.start(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (tw *timingWheel) startTickingOfPointer(ctx context.Context) {
	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		ticker := time.NewTicker(tw.config.Tick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Warning(ctx, "context canceled at ticking of pointer", nil)
				return
			case <-ticker.C:
				if !tw.IsLeader() {
					break
				}
				tw.twList.Front().Value.(*timingWheelElement).tickingOnce()
			}
		}
	}()
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
				log.Warning(ctx, "context canceled at timingwheel element heartbeat", nil)
				return
			case <-ticker.C:
				err := tw.client.heartbeat(ctx)
				if err != nil {
					log.Warning(ctx, "heartbeat failed, connection lost. try to reconnecting", map[string]interface{}{
						log.KeyError: err,
					})
				}
			}
		}
	}()
}

func (tw *timingWheel) startScheduledEventDispatcher(ctx context.Context) {
	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Warning(ctx, "context canceled at scheduled event dispatcher", nil)
				return
			default:
				if tw.IsLeader() {
					events, err := tw.receivingStation.getEvent(ctx, defaultNumberOfEventsRead)
					if err != nil {
						if !errors.Is(err, es.ErrOnEnd) {
							log.Error(ctx, "get event failed when event dispatching", map[string]interface{}{
								"eventbus":   tw.receivingStation.eventbus,
								log.KeyError: err,
							})
						}
						break
					}
					if tw.AddEvent(ctx, events[defaultIndexOfEventsRead]) {
						tw.receivingStation.offset++
						if err = tw.receivingStation.updateOffsetMeta(ctx); err != nil {
							log.Warning(ctx, "update receiving station offset metadata failed", map[string]interface{}{
								log.KeyError: err,
								"offset":     tw.receivingStation.offset,
								"eventbus":   tw.receivingStation.eventbus,
							})
						}
					}
				}
			}
		}
	}()
}

// timingWheelElement timingwheelelement has N number of buckets, every bucket is an eventbus.
type timingWheelElement struct {
	config   *Config
	kvStore  kv.Client
	client   *ctrlClient
	tick     time.Duration
	pointer  int64
	layer    int64
	interval time.Duration
	buckets  []*bucket

	leader bool
	tickC  chan struct{}
	exitC  chan struct{}
	wg     sync.WaitGroup

	timingwheel *list.List
	element     *list.Element
}

// newTimingWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimingWheelElement(c *Config, store kv.Client, cli *ctrlClient,
	tick time.Duration, layer int64) *timingWheelElement {
	if tick <= 0 {
		panic(errors.New("tick must be greater than or equal to 1s"))
	}

	var buckets []*bucket
	if layer <= c.Layers {
		buckets = createBucketsForTimingWheel(c, store, cli, tick, layer)
	}

	return &timingWheelElement{
		config:   c,
		kvStore:  store,
		client:   cli,
		tick:     tick,
		pointer:  0,
		layer:    layer,
		interval: tick * time.Duration(c.WheelSize),
		buckets:  buckets,
		tickC:    make(chan struct{}),
		exitC:    make(chan struct{}),
	}
}

func (twe *timingWheelElement) setLeader(isLeader bool) {
	twe.leader = isLeader
}

func (twe *timingWheelElement) isLeader() bool {
	return twe.leader
}

func (twe *timingWheelElement) calculateIndex(expiration, currentTime time.Time) int64 {
	var (
		pointer int64
	)
	subTick := expiration.Sub(currentTime)
	if twe.layer == 1 {
		pointer = int64(subTick/twe.tick) + twe.pointer
	} else {
		lowerTimingWheelTick := twe.element.Prev().Value.(*timingWheelElement).getTick()
		lowerTimingWheelPointer := twe.element.Prev().Value.(*timingWheelElement).getPointer()
		offset := int64((subTick + time.Duration(lowerTimingWheelPointer)*lowerTimingWheelTick) / twe.tick)
		remainder := int64((subTick + time.Duration(lowerTimingWheelPointer)*lowerTimingWheelTick) % twe.tick)
		if remainder > 0 {
			offset++
		}
		pointer = offset + twe.pointer - 1
	}
	if twe.layer <= twe.config.Layers && pointer >= twe.config.WheelSize {
		pointer %= twe.config.WheelSize
	}
	return pointer
}

func (twe *timingWheelElement) resetBucketsCapacity(newCap int64) {
	newBuckets := make([]*bucket, newCap)
	copy(newBuckets, twe.buckets)
	twe.buckets = newBuckets
}

func (twe *timingWheelElement) addEvent(ctx context.Context, tm *timingMsg) bool {
	var err error
	now := time.Now().UTC()
	if twe.layer > twe.config.Layers {
		// Put it into its own bucket
		index := twe.calculateIndex(tm.expiration, now)
		if cap(twe.buckets) < int(index+1) {
			twe.resetBucketsCapacity(index + 1)
		}
		if twe.buckets[index] == nil {
			ebName := fmt.Sprintf(timerBuiltInEventbus, twe.layer, index)
			twe.buckets[index] = newBucket(twe.config, twe.kvStore, twe.client, twe.tick, ebName, twe.layer, index)
		}

		err := twe.buckets[index].add(ctx, tm, true)
		if err != nil {
			log.Error(ctx, "add event to eventbus failed", map[string]interface{}{
				"eventbus":   twe.buckets[index].eventbus,
				"expiration": tm.expiration,
			})
			return false
		}
		log.Info(ctx, "add event to eventbus success", map[string]interface{}{
			"eventbus":   twe.buckets[index].eventbus,
			"expiration": tm.expiration,
		})
		return true
	}

	if now.After(tm.expiration) {
		// Already expired
		return tm.consume(ctx, twe.config.CtrlEndpoints) == nil
	}

	if now.Add(twe.interval).After(tm.expiration) {
		// Put it into its own bucket
		index := twe.calculateIndex(tm.expiration, now)
		err = twe.buckets[index].add(ctx, tm, false)
		if err != nil {
			log.Error(ctx, "add event to eventbus failed", map[string]interface{}{
				"eventbus":   twe.buckets[index].eventbus,
				"expiration": tm.expiration,
			})
			return false
		}
		log.Info(ctx, "add event to eventbus success", map[string]interface{}{
			"eventbus":   twe.buckets[index].eventbus,
			"expiration": tm.expiration,
		})
		return true
	}
	// Out of the interval. Put it into the overflow wheel
	return twe.next().addEvent(ctx, tm)
}

func (twe *timingWheelElement) isExistBucket(ctx context.Context, index int64) bool {
	if twe.layer <= twe.config.Layers {
		return true
	}
	if cap(twe.buckets) < int(index+1) {
		log.Debug(ctx, "the bucket of current layer capacity less than index", map[string]interface{}{
			"layer":   twe.layer,
			"pointer": twe.pointer,
			"index":   index,
		})
		return false
	}
	if twe.buckets[index] == nil {
		log.Debug(ctx, "the bucket of current layer is nil", map[string]interface{}{
			"layer":   twe.layer,
			"pointer": twe.pointer,
			"index":   index,
		})
		return false
	}
	return true
}

func (twe *timingWheelElement) fetchEventFromOverflowWheelAdvance(ctx context.Context) {
	var (
		// position of overflow wheel pointer when start loading.
		startLoadingPointer int64
		// the bucket index to be loading.
		indexToBeLoading int64
		// position of overflow wheel pointer when end loading.
		endLoadingPointer int64
	)

	startLoadingPointer = twe.next().pointer
	indexToBeLoading = (startLoadingPointer + 1) % twe.config.WheelSize
	endLoadingPointer = (startLoadingPointer + defaultFetchEventFromOverflowWheelEndPointer) % twe.config.WheelSize

	log.Debug(ctx, "start loading from overflowWheel", map[string]interface{}{
		"layer":       twe.next().layer,
		"bucketIndex": indexToBeLoading,
	})
	waitCtx, cancel := context.WithCancel(ctx)
	twe.wg.Add(1)
	go func() {
		defer twe.wg.Done()
		ticker := time.NewTicker(twe.tick / defaultLoadTriggerPeriodRatio)
		defer ticker.Stop()
		for {
			select {
			case <-waitCtx.Done():
				return
			case <-ticker.C:
				if twe.next().isExistBucket(ctx, indexToBeLoading) {
					twe.next().buckets[indexToBeLoading].fetchEventFromOverflowWheelAdvance(waitCtx, twe.addEvent)
				}
			}
		}
	}()
	wait.Until(func() {
		if twe.next().pointer == endLoadingPointer {
			log.Debug(ctx, "end loading from overflowwheel", map[string]interface{}{
				"layer":       twe.next().layer,
				"bucketIndex": indexToBeLoading,
			})
			cancel()
		}
	}, twe.tick/defaultLoadCheckWaitingPeriodRatio, waitCtx.Done())
}

func (twe *timingWheelElement) startScheduledEventDistributer(ctx context.Context) {
	for i := int64(0); i < twe.config.WheelSize; i++ {
		twe.wg.Add(1)
		go func(index int64) {
			defer twe.wg.Done()
			for {
				select {
				case <-ctx.Done():
					log.Warning(ctx, "context canceled at timingwheel element consume routine", map[string]interface{}{
						"index": index,
					})
					return
				default:
					if twe.isLeader() {
						twe.buckets[index].buildTimingMessageTask(ctx)
					}
					time.Sleep(twe.tick / defaultFlushPeriodRatio)
				}
			}
		}(i)
	}
}

func (twe *timingWheelElement) updatePointerMeta(ctx context.Context) error {
	key := fmt.Sprintf("%s/pointer/%d", metadata.MetadataKeyPrefixInKVStore, twe.layer)
	pointerMeta := &metadata.PointerMeta{
		Layer:   twe.layer,
		Pointer: twe.pointer,
	}
	data, _ := json.Marshal(pointerMeta)
	err := twe.kvStore.Set(ctx, key, data)
	if err != nil {
		log.Warning(ctx, "set pointer metadata to kvstore failed", map[string]interface{}{
			log.KeyError: err,
			"key":        key,
			"data":       data,
		})
		return err
	}
	return nil
}

func (twe *timingWheelElement) start(ctx context.Context) {
	twe.wg.Add(1)
	go func() {
		defer twe.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Warning(ctx, "context canceled at timingwheel element pointer routine", map[string]interface{}{
					"layer": twe.layer,
				})
				return
			case <-twe.tickC:
				if !twe.isLeader() {
					break
				}
				twe.pointer++
				if twe.layer <= twe.config.Layers && twe.pointer >= twe.config.WheelSize {
					twe.next().tickingOnce()
					twe.pointer = 0
				}
				if err := twe.updatePointerMeta(ctx); err != nil {
					log.Warning(ctx, "update pointer metadata failed", map[string]interface{}{
						log.KeyError: err,
						"layer":      twe.layer,
						"pointer":    twe.pointer,
					})
				}
				log.Debug(ctx, "current layer timingwheel pointer", map[string]interface{}{
					"layer":   twe.layer,
					"pointer": twe.pointer,
				})
				if twe.layer <= twe.config.Layers && twe.pointer == twe.config.WheelSize-1 {
					go twe.fetchEventFromOverflowWheelAdvance(ctx)
				}
			}
		}
	}()
}

func (twe *timingWheelElement) getTick() time.Duration {
	return twe.tick
}

func (twe *timingWheelElement) getPointer() int64 {
	return twe.pointer
}

func (twe *timingWheelElement) setPointer(pointer int64) {
	twe.pointer = pointer
}

func (twe *timingWheelElement) getLayer() int64 {
	return twe.layer
}

func (twe *timingWheelElement) getBucket(slot int64) *bucket {
	return twe.buckets[slot]
}

func (twe *timingWheelElement) getBuckets() []*bucket {
	return twe.buckets
}

func (twe *timingWheelElement) setBuckets(slot int64, b *bucket) {
	twe.buckets[slot] = b
}

func (twe *timingWheelElement) tickingOnce() {
	twe.tickC <- struct{}{}
}

func (twe *timingWheelElement) next() *timingWheelElement {
	return twe.element.Next().Value.(*timingWheelElement)
}
