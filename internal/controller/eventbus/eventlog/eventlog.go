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

package eventlog

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/block"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultAppendableSegmentNumber     = 2
	eventlogKeyPrefixInKVStore         = "/vanus/internal/resource/eventlog"
	segmentKeyPrefixInKVStore          = "/vanus/internal/resource/segment"
	eventlogSegmentsKeyPrefixInKVStore = "/vanus/internal/resource/eventlog_segments"
)

type Manager interface {
	Run(ctx context.Context, kvClient kv.Client) error
	Stop()
	AcquireEventLog(ctx context.Context, eventbusID vanus.ID) (*metadata.Eventlog, error)
	GetEventLog(ctx context.Context, id vanus.ID) *metadata.Eventlog
	GetEventLogSegmentList(elID vanus.ID) []*Segment
	GetAppendableSegment(ctx context.Context, eli *metadata.Eventlog,
		num int) ([]*Segment, error)
	UpdateSegment(ctx context.Context, m map[vanus.ID][]Segment)
}

var mgr = &eventlogManager{}

type eventlogManager struct {
	kvStore   kv.Client
	allocator block.Allocator

	// string, *eventlog
	eventLogMap sync.Map

	// vanus.ID *metadata.Eventlog
	availableLog *skiplist.SkipList
	kvMutex      sync.Mutex
	volMgr       volume.Manager
	kvClient     kv.Client
	cancel       func()
	cancelCtx    context.Context
	mutex        sync.Mutex
	// vanus.ID *Segment
	segmentNeedBeClean sync.Map
}

func NewManager(volMgr volume.Manager) Manager {
	mgr.volMgr = volMgr
	return mgr
}

func (mgr *eventlogManager) Run(ctx context.Context, kvClient kv.Client) error {
	mgr.kvClient = kvClient
	mgr.cancelCtx, mgr.cancel = context.WithCancel(ctx)
	mgr.availableLog = skiplist.New(skiplist.Uint64)
	mgr.allocator = block.NewAllocator(block.NewVolumeRoundRobin(mgr.volMgr.GetAllVolume))
	if err := mgr.allocator.Run(ctx, mgr.kvStore); err != nil {
		return err
	}
	pairs, err := mgr.kvStore.List(ctx, eventlogKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		elMD := &metadata.Eventlog{}
		err := json.Unmarshal(pair.Value, elMD)
		if err != nil {
			return err
		}
		el, err := newEventlog(ctx, elMD, mgr.kvClient, true)
		if err != nil {
			return err
		}
		mgr.eventLogMap.Store(elMD.ID, el)
	}
	go mgr.dynamicScaleUpEventLog()
	go mgr.cleanAbnormalSegment()
	return mgr.loadSegments(ctx)
}

func (mgr *eventlogManager) Stop() {
	mgr.stop()
	mgr.allocator.Stop()
}

func (mgr *eventlogManager) AcquireEventLog(ctx context.Context, eventbusID vanus.ID) (*metadata.Eventlog, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	ele := mgr.availableLog.Front()
	if ele == nil {
		if err := mgr.createEventLog(ctx); err != nil {
			return nil, err
		}
	}
	ele = mgr.availableLog.Front()
	if ele == nil {
		return nil, errors.ErrNoAvailableEventLog
	}
	elMD := ele.Value.(*metadata.Eventlog)
	elMD.EventbusID = eventbusID
	el, err := mgr.initializeEventLog(ctx, elMD)
	if err != nil {
		elMD.EventbusID = vanus.EmptyID()
		return nil, err
	}
	mgr.eventLogMap.Store(el.md.ID, el)
	mgr.availableLog.Remove(ele)
	return elMD, nil
}

func (mgr *eventlogManager) GetEventLog(ctx context.Context, id vanus.ID) *metadata.Eventlog {
	v, exist := mgr.eventLogMap.Load(id)

	if exist {
		return v.(*metadata.Eventlog)
	}
	return nil
}

func (mgr *eventlogManager) UpdateEventLog(ctx context.Context, els ...*metadata.Eventlog) error {
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	for idx := range els {
		el := els[idx]
		data, _ := json.Marshal(el)
		if err := mgr.kvStore.Set(ctx, mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *eventlogManager) GetAppendableSegment(ctx context.Context,
	eli *metadata.Eventlog, num int) ([]*Segment, error) {
	result := make([]*Segment, 0)

	if eli == nil || num == 0 {
		return result, nil
	}
	v, exist := mgr.eventLogMap.Load(eli.ID)
	if !exist {
		return nil, errors.ErrEventLogNotFound
	}

	el := v.(*eventlog)
	s := el.currentAppendableSegment()
	for len(result) < num && s != nil {
		result = append(result, s)
		s = el.nextOf(s)
	}
	return result, nil
}

func (mgr *eventlogManager) getEventLogKeyInKVStore(elID vanus.ID) string {
	return strings.Join([]string{eventlogKeyPrefixInKVStore, fmt.Sprintf("%d", elID)}, "/")
}

func (mgr *eventlogManager) UpdateSegment(ctx context.Context, m map[vanus.ID][]Segment) {
	for id, segments := range m {
		v, exist := mgr.eventLogMap.Load(id)
		if !exist {
			segmentIDs := make([]string, 0)
			for idx := range segments {
				segmentIDs = append(segmentIDs, segments[idx].ID.String())
			}
			log.Warning(ctx, "eventlog not found", map[string]interface{}{
				"id":       id,
				"segments": segmentIDs,
			})
			continue
		}
		el := v.(*eventlog)
		for idx := range segments {
			seg := el.get(segments[idx].ID)
			if seg == nil {
				log.Warning(ctx, "segment not found in eventlog", map[string]interface{}{
					"id":      id,
					"segment": seg.String(),
				})
				continue
			}
			if seg.isNeedUpdate(segments[idx]) {
				data, _ := json.Marshal(seg)
				key := filepath.Join(segmentKeyPrefixInKVStore, seg.ID.String())
				if err := mgr.kvClient.Set(ctx, key, data); err != nil {
					log.Warning(ctx, "update segment's metadata failed", map[string]interface{}{
						log.KeyError: err,
						"segment":    seg.String(),
					})
				}
			}
		}
	}
}

func (mgr *eventlogManager) GetEventLogSegmentList(elID vanus.ID) []*Segment {
	result := make([]*Segment, 0)
	v, exist := mgr.eventLogMap.Load(elID)
	if !exist {
		return result
	}
	el := v.(*eventlog)
	s := el.head()
	for s != nil {
		result = append(result, s)
		s = el.nextOf(s)
	}
	return result
}

func (mgr *eventlogManager) stop() {
	mgr.cancel()
}

func (mgr *eventlogManager) loadSegments(ctx context.Context) error {
	var err error
	mgr.eventLogMap.Range(func(key, value interface{}) bool {
		el := value.(*eventlog)
		var data []byte
		for _, v := range el.segments {
			data, err = mgr.kvClient.Get(ctx, filepath.Join(segmentKeyPrefixInKVStore, v.String()))
			if err != nil {
				return false
			}
			seg := &Segment{}
			if err = json.Unmarshal(data, seg); err != nil {
				err = errors.ErrUnmarshall.Wrap(err)
				return false
			}
		}
		return true
	})
	return err
}

func (mgr *eventlogManager) createEventLog(ctx context.Context) error {
	el := &metadata.Eventlog{
		ID: vanus.NewID(),
	}
	data, _ := json.Marshal(el)
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	if err := mgr.kvStore.Set(ctx, mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
		return err
	}
	mgr.availableLog.Set(el.ID, el)
	return nil
}

func (mgr *eventlogManager) getSegmentAddress(segs ...*Segment) []string {
	var addrs []string
	for _, v := range segs {
		ins := mgr.volMgr.GetVolumeInstanceByID(v.GetLeaderBlock().VolumeID)
		if ins == nil {
			// return []string{""} here for avoiding error handler
			return []string{""}
		}
		addrs = append(addrs, ins.Address())
	}
	return addrs
}

func (mgr *eventlogManager) initializeEventLog(ctx context.Context, md *metadata.Eventlog) (*eventlog, error) {
	el, err := newEventlog(ctx, md, mgr.kvClient, false)
	if err != nil {
		return nil, err
	}
	mgr.eventLogMap.Store(md.ID, el)
	for idx := 0; idx < defaultAppendableSegmentNumber; idx++ {
		seg, err := mgr.createSegment(ctx, el)
		if err != nil {
			return nil, err
		}

		if err = el.add(ctx, seg); err != nil {
			// preparing to cleaning
			log.Warning(ctx, "add new segment to eventlog failed when initialized", map[string]interface{}{
				log.KeyError:  err,
				"eventlog_id": el.md.ID,
			})
			mgr.segmentNeedBeClean.Store(seg.ID, seg)
			return nil, err
		}
	}
	return el, nil
}

func (mgr *eventlogManager) dynamicScaleUpEventLog() {
	ctx := context.Background()
	for {
		select {
		case <-mgr.cancelCtx.Done():
			log.Info(ctx, "the task of dynamic-scale stopped", nil)
			return
		default:
		}
		count := 0
		mgr.eventLogMap.Range(func(key, value interface{}) bool {
			el, ok := value.(*eventlog)
			if ok {
				log.Error(ctx, "assert failed in dynamicScaleUpEventLog", map[string]interface{}{
					"key": key,
				})
				return true
			}
			if el.appendableSegmentNumber() < defaultAppendableSegmentNumber {
				seg, err := mgr.createSegment(ctx, el)
				if err != nil {
					log.Warning(ctx, "create new segment failed", map[string]interface{}{
						log.KeyError:  err,
						"eventlog_id": el.md.ID,
					})
					return true
				}

				if err = el.add(ctx, seg); err != nil {
					// preparing to cleaning
					log.Warning(ctx, "add new segment to eventlog failed when scale", map[string]interface{}{
						log.KeyError:  err,
						"eventlog_id": el.md.ID,
					})
					mgr.segmentNeedBeClean.Store(seg.ID, seg)
					return true
				}
				count++
			}
			return true
		})
		log.Info(ctx, "scale task completed", map[string]interface{}{
			"segment_created": count,
		})
		time.Sleep(10 * time.Second)
	}
}

func (mgr *eventlogManager) cleanAbnormalSegment() {
	ctx := context.Background()
	for {
		select {
		case <-mgr.cancelCtx.Done():
			log.Info(ctx, "the task of clean-abnormal-segment stopped", nil)
			return
		default:
		}
		count := 0
		mgr.segmentNeedBeClean.Range(func(key, value interface{}) bool {
			v, ok := value.(*Segment)
			if !ok {
				return true
			}
			deleteKey := filepath.Join(segmentKeyPrefixInKVStore, v.ID.String())
			if err := mgr.kvClient.Delete(ctx, deleteKey); err != nil {
				log.Warning(ctx, "clean segment data in KV failed", map[string]interface{}{
					log.KeyError: deleteKey,
					"segment":    v.String(),
				})
			}
			count++
			return true
		})
		log.Info(ctx, "clean segment task completed", map[string]interface{}{
			"segment_cleaned": count,
		})
		time.Sleep(time.Minute)
	}
}

func (mgr *eventlogManager) createSegment(ctx context.Context, el *eventlog) (*Segment, error) {
	seg, err := mgr.generateSegment(ctx)
	if err != nil {
		return nil, err
	}
	seg.EventLogID = el.md.ID

	for i := 1; i <= 3; i++ {
		seg.Replicas.Leader = i
		ins := mgr.volMgr.GetVolumeInstanceByID(seg.GetLeaderBlock().VolumeID)
		_, err := ins.GetClient().ActivateSegment(ctx, &segment.ActivateSegmentRequest{
			EventLogId:     seg.EventLogID.Uint64(),
			ReplicaGroupId: seg.Replicas.ID.Uint64(),
			PeersAddress:   mgr.getSegmentAddress(seg),
		})
		if err == nil {
			break
		}
	}

	seg.State = StateWorking
	data, _ := json.Marshal(seg)
	key := filepath.Join(segmentKeyPrefixInKVStore, seg.ID.String())
	if err = mgr.kvClient.Set(ctx, key, data); err != nil {
		log.Error(ctx, "update segment's metadata failed", map[string]interface{}{
			log.KeyError: err,
			"segment":    seg.String(),
		})
		// preparing to cleaning
		mgr.segmentNeedBeClean.Store(seg.ID, seg)
		return nil, err
	}

	return seg, nil
}

func (mgr *eventlogManager) generateSegment(ctx context.Context) (*Segment, error) {
	var seg *Segment
	blocks, err := mgr.allocator.Pick(ctx, 3)
	if err != nil {
		return nil, err
	}
	blockMap := make(map[int]*metadata.Block)
	for idx, v := range blocks {
		blockMap[idx] = v
	}
	seg = &Segment{
		ID:       vanus.NewID(),
		Capacity: blocks[0].Capacity,
		Replicas: &ReplicaGroup{
			ID:        vanus.NewID(),
			Peers:     blockMap,
			CreateAt:  time.Now(),
			DestroyAt: time.Now(),
		},
		State: StateCreated,
	}

	data, _ := json.Marshal(seg)
	if err = mgr.kvClient.Set(ctx, seg.ID.String(), data); err != nil {
		log.Error(ctx, "save segment's data to kv store failed", map[string]interface{}{
			log.KeyError: err,
			"segment":    seg.String(),
		})
		mgr.allocator.Clean(ctx, blocks...)
		return nil, err
	}
	return seg, nil
}

type eventlog struct {
	segmentList *skiplist.SkipList
	md          *metadata.Eventlog
	writePtr    *Segment
	kvClient    kv.Client
	mutex       sync.RWMutex
	segments    []vanus.ID
}

// newEventlog create an object in memory. if needLoad is true, there will read metadata
//from kv store. the eventlog.segmentList should be built after call this method
func newEventlog(ctx context.Context, md *metadata.Eventlog, kvClient kv.Client, needLoad bool) (*eventlog, error) {
	el := &eventlog{
		segmentList: skiplist.New(skiplist.Uint64),
		md:          &metadata.Eventlog{},
		kvClient:    kvClient,
		segments:    []vanus.ID{},
	}
	if !needLoad {
		return el, nil
	}
	data, err := kvClient.Get(ctx, filepath.Join(eventlogKeyPrefixInKVStore, md.ID.String()))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, el.md); err != nil {
		return nil, errors.ErrUnmarshall.Wrap(err)
	}
	pairs, err := kvClient.List(ctx, filepath.Join(eventlogSegmentsKeyPrefixInKVStore, md.ID.String()))
	if err != nil {
		return nil, err
	}
	var segmentIDs []vanus.ID
	for _, v := range pairs {
		id := new(struct {
			SegmentID vanus.ID `json:"segment_id"`
		})
		if err := json.Unmarshal(v.Value, id); err != nil {
			return nil, errors.ErrUnmarshall.Wrap(err)
		}
		segmentIDs = append(segmentIDs, id.SegmentID)
	}
	el.segments = segmentIDs
	return el, nil
}

func (el *eventlog) get(id vanus.ID) *Segment {
	v := el.segmentList.Get(id)
	if v == nil {
		return nil
	}
	return v.Value.(*Segment)
}

func (el *eventlog) appendableSegmentNumber() int {
	el.mutex.RLock()
	defer el.mutex.Unlock()
	if el.writePtr == nil {
		return 0
	}
	head := el.writePtr
	count := 0
	for head != nil {
		count++
	}
	return count
}

func (el *eventlog) currentAppendableSegment() *Segment {
	el.mutex.RLock()
	defer el.mutex.Unlock()
	if el.size() == 0 {
		return nil
	}
	if el.writePtr == nil {
		head := el.segmentList.Front()
		for head != nil {
			s := head.Value.(*Segment)
			if s.IsAppendable() {
				el.writePtr = s
				break
			}
			head = head.Next()
		}
	}
	return el.writePtr
}

// add a segment to eventlog, the metadata of this eventlog will be updated, but the segment's metadata should be
// updated after call this method
func (el *eventlog) add(ctx context.Context, seg *Segment) error {
	el.mutex.Lock()
	defer el.mutex.Unlock()

	last := el.tail()
	if last != nil {
		last.NextSegmentId = seg.ID
		seg.PreviousSegmentId = last.ID
	}
	s := new(struct {
		SegmentID vanus.ID `json:"segment_id"`
	})
	s.SegmentID = seg.ID
	data, _ := json.Marshal(s)
	key := filepath.Join(eventlogSegmentsKeyPrefixInKVStore, el.md.ID.String(), seg.ID.String())
	if err := el.kvClient.Set(ctx, key, data); err != nil {
		last.NextSegmentId = vanus.EmptyID()
		seg.PreviousSegmentId = vanus.EmptyID()
		return err
	}

	el.segments = append(el.segments, seg.ID)
	el.segmentList.Set(seg.ID.Uint64(), el)
	return nil
}

func (el *eventlog) head() *Segment {
	el.mutex.RLock()
	defer el.mutex.Unlock()

	if el.size() == 0 {
		return nil
	}
	ptr := el.segmentList.Front()
	return ptr.Value.(*Segment)
}

func (el *eventlog) tail() *Segment {
	el.mutex.RLock()
	defer el.mutex.Unlock()

	if el.size() == 0 {
		return nil
	}
	ptr := el.segmentList.Back()
	return ptr.Value.(*Segment)
}

func (el *eventlog) indexAt(idx int) *Segment {
	el.mutex.RLock()
	defer el.mutex.Unlock()

	if el.size() < idx {
		return nil
	}
	ptr := el.segmentList.Front()
	for i := 0; i < idx; i++ {
		ptr = ptr.Next()
	}
	return ptr.Value.(*Segment)
}

func (el *eventlog) size() int {
	el.mutex.RLock()
	defer el.mutex.Unlock()

	return el.segmentList.Len()
}

func (el *eventlog) nextOf(seg *Segment) *Segment {
	if seg == nil {
		return nil
	}
	el.mutex.RLock()
	defer el.mutex.Unlock()

	node := el.segmentList.Get(seg.ID)
	next := node.Next()
	if next == nil {
		return nil
	}
	return next.Value.(*Segment)
}

func (el *eventlog) previousOf(seg *Segment) *Segment {
	if seg == nil {
		return nil
	}
	el.mutex.RLock()
	defer el.mutex.Unlock()

	node := el.segmentList.Get(seg.ID)
	prev := node.Prev()
	if prev == nil {
		return nil
	}
	return prev.Value.(*Segment)
}
