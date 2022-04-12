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

package trigger

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/worker"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

const (
	defaultHeartbeatTimeout  = 2 * time.Minute
	defaultDisconnectTimeout = 30 * time.Minute
)

type OnTriggerWorkerRemoveSubscription func(ctx context.Context, subId, addr string) error

type TriggerWorkerManager struct {
	triggerWorkers       map[string]*worker.TriggerWorker
	storage              storage.TriggerWorkerStorage
	subscriptionManager  *SubscriptionManager
	lock                 sync.RWMutex
	onRemoveSubscription OnTriggerWorkerRemoveSubscription
}

func NewTriggerWorkerManager(storage storage.TriggerWorkerStorage, subscriptionManager *SubscriptionManager, handler OnTriggerWorkerRemoveSubscription) *TriggerWorkerManager {
	m := &TriggerWorkerManager{
		storage:              storage,
		subscriptionManager:  subscriptionManager,
		triggerWorkers:       map[string]*worker.TriggerWorker{},
		onRemoveSubscription: handler,
	}
	return m
}

func (m *TriggerWorkerManager) AddSubscription(ctx context.Context, twInfo info.TriggerWorkerInfo, subId string) error {
	tWorker := m.getTriggerWorker(twInfo.Addr)
	if tWorker == nil || tWorker.Info.Phase != info.TriggerWorkerPhaseRunning {
		return errors.ErrResourceNotFound
	}
	subData := m.subscriptionManager.GetSubscription(ctx, subId)
	if subData == nil {
		return errors.ErrResourceNotFound
	}
	offsets, err := m.subscriptionManager.GetOffset(ctx, subId)
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
	tWorker.AddSub(sub.ID)
	log.Info(ctx, "trigger worker add a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetSubIds(),
	})
	return nil
}

func (m *TriggerWorkerManager) RemoveSubscription(ctx context.Context, addr string, subId string) error {
	tWorker := m.getTriggerWorker(addr)
	if tWorker == nil || tWorker.Info.Phase == info.TriggerWorkerPhasePaused {
		return nil
	}
	if tWorker.Info.Phase == info.TriggerWorkerPhaseRunning {
		err := tWorker.RemoveSubscriptions(ctx, subId)
		if err != nil {
			return err
		}
	}
	tWorker.RemoveSub(subId)
	log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetSubIds(),
	})
	return nil
}

func (m *TriggerWorkerManager) doTriggerWorkerLeave(ctx context.Context, tWorker *worker.TriggerWorker) {
	subIds := tWorker.GetSubIds()
	//reallocate subscription
	for subId := range subIds {
		err := m.onRemoveSubscription(ctx, subId, tWorker.Info.Addr)
		if err != nil {
			log.Error(ctx, "trigger worker leave on remove subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeySubscriptionID:    subId,
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			})
		} else {
			tWorker.RemoveSub(subId)
			log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
				log.KeySubscriptionID:    subId,
				"subIds":                 tWorker.GetSubIds(),
			})
		}
	}
}

func (m *TriggerWorkerManager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		twInfo := info.NewTriggerWorkerInfo(addr)
		tWorker = worker.NewTriggerWorker(twInfo)
		m.triggerWorkers[twInfo.Addr] = tWorker
	} else {
		log.Info(ctx, "repeat add trigger worker", map[string]interface{}{
			"triggerWorker": tWorker.Info,
		})
		if tWorker.Info.Phase == info.TriggerWorkerPhasePaused {
			return errors.ErrResourceAlreadyExist
		}
		tWorker.ResetReportSubId()
		tWorker.Info.Phase = info.TriggerWorkerPhasePending
	}
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.Info)
	if err != nil {
		return err
	}
	go func() {
		err := tWorker.Start(ctx)
		if err != nil {
			log.Warning(ctx, "trigger worker start error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			})
		} else {
			log.Info(ctx, "trigger worker start success", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			})
		}
	}()
	log.Info(ctx, "add trigger worker", map[string]interface{}{
		"triggerWorker": tWorker.Info,
	})
	return nil
}

func (m *TriggerWorkerManager) RemoveTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist || tWorker.Info.Phase == info.TriggerWorkerPhasePaused {
		log.Info(ctx, "remove trigger worker not exist or phase is paused", map[string]interface{}{
			log.KeyTriggerWorkerAddr: addr,
		})
		return nil
	}
	tWorker.Info.Phase = info.TriggerWorkerPhasePaused
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.Info)
	if err != nil {
		return err
	}
	err = m.cleanTriggerWorker(ctx, tWorker)
	if err != nil {
		log.Warning(ctx, "remove trigger worker clean error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		})
	} else {
		log.Info(ctx, "remove trigger worker success", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		})
	}
	return nil
}

func (m *TriggerWorkerManager) UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[string]struct{}) bool {
	tWorker := m.getTriggerWorker(addr)
	if tWorker == nil || tWorker.Info.Phase == info.TriggerWorkerPhasePaused {
		return false
	}
	phase := tWorker.Info.Phase
	if phase != info.TriggerWorkerPhaseRunning {
		tWorker.Info.Phase = info.TriggerWorkerPhaseRunning
		err := m.storage.SaveTriggerWorker(ctx, *tWorker.Info)
		if err != nil {
			log.Warning(ctx, "save trigger worker phase to running error", map[string]interface{}{
				log.KeyError: err,
				"phase":      phase,
			})
			//save fail then modify memory to original,then last will save too
			tWorker.Info.Phase = phase
		} else {
			log.Info(ctx, "trigger worker phase to running", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			})
		}
	}
	tWorker.Info.HeartbeatTime = time.Now()
	tWorker.SetReportSubId(subIds)
	return true
}

func (m *TriggerWorkerManager) GetRunningTriggerWorker() []info.TriggerWorkerInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var runningTriggerWorker []info.TriggerWorkerInfo
	for _, tWorker := range m.triggerWorkers {
		twInfo := tWorker.Info
		if twInfo.Phase == info.TriggerWorkerPhaseRunning {
			continue
		}
		runningTriggerWorker = append(runningTriggerWorker, *twInfo)
	}
	return runningTriggerWorker
}

func (m *TriggerWorkerManager) getTriggerWorker(addr string) *worker.TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		return nil
	}
	return tWorker
}

func (m *TriggerWorkerManager) getTriggerWorkers() map[string]*worker.TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.triggerWorkers
}

func (m *TriggerWorkerManager) Init(ctx context.Context) error {
	tWorkerInfos, err := m.storage.ListTriggerWorker(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "trigger worker size", map[string]interface{}{"size": len(tWorkerInfos)})
	for i := range tWorkerInfos {
		twInfo := tWorkerInfos[i]
		twInfo.HeartbeatTime = time.Now()
		tWorker := worker.NewTriggerWorker(twInfo)
		m.triggerWorkers[twInfo.Addr] = tWorker
	}
	subscriptions := m.subscriptionManager.ListSubscription(ctx)
	for subId, subData := range subscriptions {
		if subData.TriggerWorker != "" {
			tWorker, exist := m.triggerWorkers[subData.TriggerWorker]
			if exist {
				tWorker.AddSub(subId)
			} else {
				//不应该出现这种情况
				log.Error(ctx, "init trigger worker subscription no found trigger worker", map[string]interface{}{
					log.KeySubscriptionID:    subId,
					log.KeyTriggerWorkerAddr: subData.TriggerWorker,
				})
			}
		}
	}
	return nil
}

func (m *TriggerWorkerManager) Run(ctx context.Context) {
	go func() {
		util.UntilWithContext(ctx, m.check, time.Second)
	}()
}

func (m *TriggerWorkerManager) check(ctx context.Context) {
	var wg sync.WaitGroup
	now := time.Now()
	workers := m.getTriggerWorkers()
	for _, tWorker := range workers {
		wg.Add(1)
		go func(tWorker *worker.TriggerWorker) {
			twInfo := tWorker.Info
			defer wg.Done()
			switch twInfo.Phase {
			case info.TriggerWorkerPhaseRunning:
				if now.Sub(twInfo.HeartbeatTime) > defaultHeartbeatTimeout {
					twInfo.Phase = info.TriggerWorkerPhaseDisconnect
					err := m.storage.SaveTriggerWorker(ctx, *twInfo)
					if err != nil {
						log.Warning(ctx, "running trigger worker heartbeat timeout save error", map[string]interface{}{
							log.KeyError:    err,
							"triggerWorker": twInfo,
						})
					} else {
						log.Info(ctx, "running trigger worker heartbeat timeout", map[string]interface{}{
							"triggerWorker": twInfo,
						})
					}
				} else {
					m.compareTriggerWorkerSub(ctx, tWorker)
				}
			case info.TriggerWorkerPhasePending:
				err := tWorker.Start(ctx)
				if err != nil {
					log.Warning(ctx, "trigger worker start error", map[string]interface{}{
						log.KeyError:             err,
						log.KeyTriggerWorkerAddr: twInfo.Addr,
					})
				} else {
					log.Info(ctx, "trigger worker start success", map[string]interface{}{
						log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
					})
				}
			case info.TriggerWorkerPhasePaused:
				err := m.cleanTriggerWorker(ctx, tWorker)
				if err != nil {
					log.Warning(ctx, "pause trigger worker clean error", map[string]interface{}{
						log.KeyError:             err,
						log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
					})
				}
			case info.TriggerWorkerPhaseDisconnect:
				if now.Sub(twInfo.HeartbeatTime) > defaultDisconnectTimeout {
					log.Info(ctx, "trigger worker disconnect timeout", map[string]interface{}{
						"triggerWorker": twInfo,
					})
					err := m.cleanTriggerWorker(ctx, tWorker)
					if err != nil {
						log.Warning(ctx, "disconnect trigger worker clean error", map[string]interface{}{
							log.KeyError:             err,
							log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
						})
					}
				}
			}
		}(tWorker)
	}
	wg.Wait()
}

func (m *TriggerWorkerManager) compareTriggerWorkerSub(ctx context.Context, tWorker *worker.TriggerWorker) {
	reportSubIds := tWorker.GetReportSubId()
	subIds := tWorker.GetSubIds()
	for subId := range subIds {
		if _, exist := reportSubIds[subId]; !exist {
			//trigger worker need but report is no,need add
			err := m.AddSubscription(ctx, *tWorker.Info, subId)
			if err != nil {
				log.Warning(ctx, "compare trigger worker subscription add subscription error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: subId,
				})
			}
		}
	}
}
func (m *TriggerWorkerManager) cleanTriggerWorker(ctx context.Context, tWorker *worker.TriggerWorker) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.doTriggerWorkerLeave(ctx, tWorker)
	err := m.storage.DeleteTriggerWorker(ctx, tWorker.Info.Id)
	if err != nil {
		return err
	}
	delete(m.triggerWorkers, tWorker.Info.Addr)
	log.Info(ctx, "trigger worker clean success", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
	})
	return nil
}
