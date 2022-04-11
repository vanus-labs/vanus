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

package worker

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/trigger/cache"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/offset"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

const (
	defaultHeartbeatTimeout  = 2 * time.Minute
	defaultDisconnectTimeout = 30 * time.Minute
)

type OnTriggerWorkerRemoveSubscription func(context.Context, string, string) error

type TriggerWorkerManager struct {
	triggerWorkers       map[string]*TriggerWorker
	offsetManager        *offset.Manager
	storage              storage.TriggerWorkerStorage
	subscriptionCache    *cache.SubscriptionCache
	lock                 sync.RWMutex
	onRemoveSubscription OnTriggerWorkerRemoveSubscription
}

func NewTriggerWorkerManager(storage storage.TriggerWorkerStorage, offsetManger *offset.Manager, handler OnTriggerWorkerRemoveSubscription) *TriggerWorkerManager {
	m := &TriggerWorkerManager{
		storage:              storage,
		offsetManager:        offsetManger,
		triggerWorkers:       map[string]*TriggerWorker{},
		onRemoveSubscription: handler,
	}
	return m
}

func (m *TriggerWorkerManager) AddSubscription(ctx context.Context, twInfo info.TriggerWorkerInfo, subId string) error {
	tWorker, exist := m.triggerWorkers[twInfo.Addr]
	if !exist || tWorker.twInfo.Phase != info.TriggerWorkerPhaseRunning {
		return errors.ErrResourceNotFound
	}
	subData, err := m.subscriptionCache.GetSubscription(ctx, subId)
	if err != nil {
		return err
	}
	offsets, err := m.offsetManager.GetOffset(ctx, subId)
	if err != nil {
		return err
	}
	sub := &primitive.Subscription{
		ID:       subData.ID,
		Filters:  subData.Filters,
		Sink:     subData.Sink,
		EventBus: subData.EventBus,
		Offsets:  offsets,
	}
	err = tWorker.AddSubscription(ctx, sub)
	if err != nil {
		return err
	}
	tWorker.twInfo.AddSub(sub.ID)
	m.storage.SaveTriggerWorker(ctx, *tWorker.twInfo)
	return nil
}

func (m *TriggerWorkerManager) RemoveSubscription(ctx context.Context, addr string, subId string) error {
	tWorker := m.getTriggerWorker(addr)
	if tWorker == nil || tWorker.twInfo.Phase == info.TriggerWorkerPhasePaused {
		return nil
	}
	if tWorker.twInfo.Phase == info.TriggerWorkerPhaseRunning {
		err := tWorker.RemoveSubscriptions(ctx, subId)
		if err != nil {
			return err
		}
	}
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.twInfo)
	if err != nil {
		return err
	}
	tWorker.twInfo.RemoveSub(subId)
	return nil
}

func (m *TriggerWorkerManager) doTriggerWorkerLeave(ctx context.Context, tWorker *TriggerWorker) bool {
	subIds := tWorker.twInfo.GetSubIds()
	if len(subIds) == 0 {
		return false
	}
	var hasFail bool
	//reallocate subscription
	for subId := range subIds {
		err := m.onRemoveSubscription(ctx, subId, tWorker.twInfo.Addr)
		if err != nil {
			tWorker.twInfo.RemoveSub(subId)
		} else {
			hasFail = true
			log.Error(ctx, "remove subscription error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: subId,
				"addr":                tWorker.twInfo.Addr,
			})
		}
	}
	return hasFail
}

func (m *TriggerWorkerManager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	twInfo := info.NewTriggerWorkerInfo(addr)
	tWorker, exist := m.triggerWorkers[twInfo.Addr]
	if !exist {
		tWorker = NewTriggerWorker(twInfo)
		m.triggerWorkers[twInfo.Addr] = tWorker
	} else {
		m.doTriggerWorkerLeave(ctx, tWorker)
		tWorker.twInfo.Phase = info.TriggerWorkerPhasePending
	}
	err := m.storage.SaveTriggerWorker(ctx, *twInfo)
	if err != nil {
		return err
	}
	err = tWorker.Start(ctx)
	if err != nil {
		return err
	}

	log.Info(ctx, "add trigger worker", map[string]interface{}{
		"triggerWorker": twInfo,
	})
	return nil
}

func (m *TriggerWorkerManager) RemoveTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		return nil
	}
	m.doTriggerWorkerLeave(ctx, tWorker)
	tWorker.twInfo.Phase = info.TriggerWorkerPhasePaused
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.twInfo)
	if err != nil {
		return err
	}
	return nil
}

func (m *TriggerWorkerManager) UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[string]struct{}) bool {
	tWorker := m.getTriggerWorker(addr)
	if tWorker == nil || tWorker.twInfo.Phase == info.TriggerWorkerPhasePaused {
		return false
	}
	phase := tWorker.twInfo.Phase
	if phase != info.TriggerWorkerPhaseRunning {
		tWorker.twInfo.Phase = info.TriggerWorkerPhaseRunning
		err := m.storage.SaveTriggerWorker(ctx, *tWorker.twInfo)
		if err != nil {
			log.Warning(ctx, "save trigger worker phase to running error", map[string]interface{}{
				log.KeyError: err,
				"phase":      phase,
			})
			//save fail then modify memory to original,then last will save too
			tWorker.twInfo.Phase = phase
		}
	}
	tWorker.twInfo.HeartbeatTime = time.Now()
	tWorker.twInfo.SetReportSubId(subIds)
	return true
}

func (m *TriggerWorkerManager) GetRunningTriggerWorker() []info.TriggerWorkerInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var runningTriggerWorker []info.TriggerWorkerInfo
	for _, tWorker := range m.triggerWorkers {
		twInfo := tWorker.twInfo
		if twInfo.Phase == info.TriggerWorkerPhaseRunning {
			continue
		}
		runningTriggerWorker = append(runningTriggerWorker, *twInfo)
	}
	return runningTriggerWorker
}

func (m *TriggerWorkerManager) getTriggerWorker(addr string) *TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		return nil
	}
	return tWorker
}

func (m *TriggerWorkerManager) initTriggerWorkers(ctx context.Context) error {
	tWorkerInfos, err := m.storage.ListTriggerWorker(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "trigger worker size", map[string]interface{}{"size": len(tWorkerInfos)})
	for i := range tWorkerInfos {
		twInfo := tWorkerInfos[i]
		twInfo.HeartbeatTime = time.Now()
		tWorker := NewTriggerWorker(twInfo)
		m.triggerWorkers[twInfo.Addr] = tWorker
	}
	return nil
}

func (m *TriggerWorkerManager) Run(ctx context.Context) error {
	err := m.initTriggerWorkers(ctx)
	if err != nil {
		return err
	}
	go func() {
		time.Sleep(time.Second)
		m.check(ctx)
		util.UntilWithContext(ctx, m.check, time.Second*5)
	}()
	return nil
}

func (m *TriggerWorkerManager) check(ctx context.Context) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var wg sync.WaitGroup
	now := time.Now()
	for _, tWorker := range m.triggerWorkers {
		wg.Add(1)
		go func(tWorker *TriggerWorker) {
			twInfo := tWorker.twInfo
			defer wg.Done()
			switch twInfo.Phase {
			case info.TriggerWorkerPhasePending:
				err := tWorker.Start(ctx)
				if err != nil {
					log.Warning(ctx, "start trigger worker error", map[string]interface{}{
						log.KeyError: err,
						"addr":       twInfo.Addr,
					})
				}
			case info.TriggerWorkerPhaseRunning:
				if now.Sub(twInfo.HeartbeatTime) > defaultHeartbeatTimeout {
					twInfo.Phase = info.TriggerWorkerPhaseDisconnect
					m.storage.SaveTriggerWorker(ctx, *twInfo)
				} else {
					//compare sub
					m.compareTriggerWorkerSub(ctx, tWorker)
				}
			case info.TriggerWorkerPhasePaused:
				hasFail := m.doTriggerWorkerLeave(ctx, tWorker)
				if !hasFail {
					m.cleanTriggerWorker(ctx, twInfo)
				}
			case info.TriggerWorkerPhaseDisconnect:
				if now.Sub(twInfo.HeartbeatTime) > defaultDisconnectTimeout {
					hasFail := m.doTriggerWorkerLeave(ctx, tWorker)
					if !hasFail {
						m.cleanTriggerWorker(ctx, twInfo)
					}
				}
			}
		}(tWorker)

	}
	wg.Wait()
}

func (m *TriggerWorkerManager) compareTriggerWorkerSub(ctx context.Context, tWorker *TriggerWorker) {
	twInfo := tWorker.twInfo
	reportSubIds := twInfo.GetReportSubId()
	subIds := twInfo.GetSubIds()
	for reportSubId := range reportSubIds {
		if _, exist := subIds[reportSubId]; !exist {
			//trigger worker do should not do sub, need remove
			err := tWorker.RemoveSubscriptions(ctx, reportSubId)
			if err != nil {
				log.Warning(ctx, "compare trigger worker subscription remove subscription error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: reportSubId,
				})
			}
		}
	}
	for subId := range subIds {
		if _, exist := reportSubIds[subId]; !exist {
			//trigger worker need but report is no,need add
			err := m.AddSubscription(ctx, *twInfo, subId)
			if err != nil {
				log.Warning(ctx, "compare trigger worker subscription add subscription error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: subId,
				})
			}
		}
	}
}
func (m *TriggerWorkerManager) cleanTriggerWorker(ctx context.Context, twInfo *info.TriggerWorkerInfo) error {
	log.Info(ctx, "clean trigger worker start", map[string]interface{}{
		"addr": twInfo.Addr,
	})
	if tWorker, exist := m.triggerWorkers[twInfo.Addr]; exist {
		tWorker.Close()
	}
	err := m.storage.DeleteTriggerWorker(ctx, twInfo.Id)
	if err != nil {
		return err
	}
	delete(m.triggerWorkers, twInfo.Addr)
	log.Info(ctx, "clean trigger worker success", map[string]interface{}{
		"addr": twInfo.Addr,
	})
	return nil
}
