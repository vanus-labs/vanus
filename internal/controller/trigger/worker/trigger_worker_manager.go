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

//go:generate mockgen -source=trigger_worker_manager.go  -destination=testing/mock_trigger_worker_manager.go -package=testing
package worker

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

const (
	defaultCheckPeriod       = 5 * time.Second
	defaultHeartbeatTimeout  = 5 * time.Minute
	defaultDisconnectTimeout = 30 * time.Minute
)

type Manager interface {
	AssignSubscription(ctx context.Context, twInfo info.TriggerWorkerInfo, subId string) error
	UnAssignSubscription(ctx context.Context, addr string, subId string) error
	AddTriggerWorker(ctx context.Context, addr string) error
	RemoveTriggerWorker(ctx context.Context, addr string)
	UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[string]struct{}) bool
	GetRunningTriggerWorker() []info.TriggerWorkerInfo
	Init(ctx context.Context) error
	Start()
	Stop()
}

type OnTriggerWorkerRemoveSubscription func(ctx context.Context, subId, addr string) error

type manager struct {
	triggerWorkers       map[string]*TriggerWorker
	storage              storage.TriggerWorkerStorage
	subscriptionManager  subscription.Manager
	lock                 sync.RWMutex
	onRemoveSubscription OnTriggerWorkerRemoveSubscription
	ctx                  context.Context
	stop                 context.CancelFunc
}

func NewTriggerWorkerManager(storage storage.TriggerWorkerStorage, subscriptionManager subscription.Manager, handler OnTriggerWorkerRemoveSubscription) Manager {
	m := &manager{
		storage:              storage,
		subscriptionManager:  subscriptionManager,
		triggerWorkers:       map[string]*TriggerWorker{},
		onRemoveSubscription: handler,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) AssignSubscription(ctx context.Context, twInfo info.TriggerWorkerInfo, subId string) error {
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
	tWorker.AddAssignSub(sub.ID)
	log.Info(ctx, "trigger worker add a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetAssignSubIds(),
	})
	return nil
}

func (m *manager) UnAssignSubscription(ctx context.Context, addr string, subId string) error {
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
	tWorker.RemoveAssignSub(subId)
	log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetAssignSubIds(),
	})
	return nil
}

func (m *manager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		tWorker = NewTriggerWorker(info.NewTriggerWorkerInfo(addr))
		m.triggerWorkers[addr] = tWorker
		m.lock.Unlock()
	} else {
		m.lock.Unlock()
		log.Info(ctx, "repeat add trigger worker", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			"subIds":                 tWorker.GetAssignSubIds(),
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
	go func(ctx context.Context) {
		//wait trigger worker start
		time.Sleep(2 * time.Second)
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
	}(m.ctx)
	log.Info(ctx, "add trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
	})
	return nil
}

func (m *manager) RemoveTriggerWorker(ctx context.Context, addr string) {
	tWorker := m.getTriggerWorker(addr)
	if tWorker == nil || tWorker.Info.Phase == info.TriggerWorkerPhasePaused {
		log.Info(ctx, "remove trigger worker not exist or phase is paused", map[string]interface{}{
			log.KeyTriggerWorkerAddr: addr,
		})
		return
	}
	tWorker.Info.Phase = info.TriggerWorkerPhasePaused
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.Info)
	if err != nil {
		log.Warning(ctx, "trigger worker remove save phase error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: addr,
			"subIds":                 tWorker.GetAssignSubIds(),
		})
	}
	m.cleanTriggerWorker(ctx, tWorker)
}

func (m *manager) UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[string]struct{}) bool {
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
	} else {
		tWorker.Info.HeartbeatPeriod = time.Now().Sub(tWorker.Info.HeartbeatTime)
	}
	tWorker.Info.HeartbeatTime = time.Now()
	tWorker.SetReportSubId(subIds)
	return true
}

func (m *manager) cleanTriggerWorker(ctx context.Context, tWorker *TriggerWorker) {
	hasFail := m.doTriggerWorkerLeave(ctx, tWorker)
	if hasFail {
		log.Warning(ctx, "trigger worker leave remove subscription has fail", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			"subIds":                 tWorker.GetAssignSubIds(),
		})
		return
	}
	err := m.storage.DeleteTriggerWorker(ctx, tWorker.Info.Id)
	if err != nil {
		log.Warning(ctx, "delete trigger worker error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
		})
		return
	}
	m.deleteTriggerWorker(tWorker.Info.Addr)
}

func (m *manager) doTriggerWorkerLeave(ctx context.Context, tWorker *TriggerWorker) bool {
	subIds := tWorker.GetAssignSubIds()
	//reallocate subscription
	var hasFail bool
	for subId := range subIds {
		err := m.onRemoveSubscription(ctx, subId, tWorker.Info.Addr)
		if err != nil {
			hasFail = true
			log.Info(ctx, "trigger worker leave on remove subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeySubscriptionID:    subId,
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
			})
		} else {
			tWorker.RemoveAssignSub(subId)
			log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.Info.Addr,
				log.KeySubscriptionID:    subId,
				"subIds":                 tWorker.GetAssignSubIds(),
			})
		}
	}
	return hasFail
}

func (m *manager) GetRunningTriggerWorker() []info.TriggerWorkerInfo {
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

func (m *manager) getTriggerWorkers() map[string]*TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.triggerWorkers
}

func (m *manager) getTriggerWorker(addr string) *TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		return nil
	}
	return tWorker
}

func (m *manager) deleteTriggerWorker(addr string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.triggerWorkers, addr)
}

func (m *manager) Init(ctx context.Context) error {
	tWorkerInfos, err := m.storage.ListTriggerWorker(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "trigger worker size", map[string]interface{}{"size": len(tWorkerInfos)})
	for i := range tWorkerInfos {
		twInfo := tWorkerInfos[i]
		twInfo.Init()
		tWorker := NewTriggerWorker(twInfo)
		m.triggerWorkers[twInfo.Addr] = tWorker
	}
	subscriptions := m.subscriptionManager.ListSubscription(ctx)
	for subId, subData := range subscriptions {
		if subData.TriggerWorker != "" {
			tWorker, exist := m.triggerWorkers[subData.TriggerWorker]
			if exist {
				tWorker.AddAssignSub(subId)
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

func (m *manager) Stop() {
	m.stop()
}

func (m *manager) Start() {
	go util.UntilWithContext(m.ctx, m.check, defaultCheckPeriod)
}

func (m *manager) check(ctx context.Context) {
	log.Debug(ctx, "trigger worker check begin", nil)
	var wg sync.WaitGroup
	now := time.Now()
	workers := m.getTriggerWorkers()
	for _, tWorker := range workers {
		wg.Add(1)
		go func(tWorker *TriggerWorker) {
			twInfo := tWorker.Info
			defer wg.Done()
			switch twInfo.Phase {
			case info.TriggerWorkerPhaseRunning:
				m.runningTriggerWorkerHandler(ctx, tWorker)
			case info.TriggerWorkerPhasePending:
				m.pendingTriggerWorkerHandler(ctx, tWorker)
			case info.TriggerWorkerPhasePaused:
				m.cleanTriggerWorker(ctx, tWorker)
			case info.TriggerWorkerPhaseDisconnect:
				if now.Sub(twInfo.HeartbeatTime) > defaultDisconnectTimeout {
					log.Info(ctx, "trigger worker disconnect timeout", map[string]interface{}{
						log.KeyTriggerWorkerAddr: twInfo.Addr,
						"subIds":                 tWorker.GetAssignSubIds(),
					})
					m.cleanTriggerWorker(ctx, tWorker)
				}
			}
		}(tWorker)
	}
	wg.Wait()
	log.Debug(ctx, "trigger worker check complete", nil)
}

func (m *manager) pendingTriggerWorkerHandler(ctx context.Context, tWorker *TriggerWorker) {
	now := time.Now()
	twInfo := tWorker.Info
	if now.Sub(twInfo.HeartbeatTime) > defaultHeartbeatTimeout {
		twInfo.Phase = info.TriggerWorkerPhasePaused
		err := m.storage.SaveTriggerWorker(ctx, *twInfo)
		if err != nil {
			log.Warning(ctx, "pending trigger worker heartbeat timeout save error", map[string]interface{}{
				log.KeyError:    err,
				"triggerWorker": twInfo,
			})
		} else {
			log.Info(ctx, "pending trigger worker heartbeat timeout", map[string]interface{}{
				"triggerWorker": twInfo,
			})
		}
	} else {
		if now.Sub(twInfo.HeartbeatTime) < 30*time.Second {
			return
		}
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
	}
}

func (m *manager) runningTriggerWorkerHandler(ctx context.Context, tWorker *TriggerWorker) {
	now := time.Now()
	twInfo := tWorker.Info
	if now.Sub(twInfo.HeartbeatTime) > defaultHeartbeatTimeout {
		twInfo.Phase = info.TriggerWorkerPhaseDisconnect
		err := m.storage.SaveTriggerWorker(ctx, *twInfo)
		if err != nil {
			log.Info(ctx, "running trigger worker heartbeat timeout save error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				"subIds":                 tWorker.GetAssignSubIds(),
			})
		} else {
			log.Info(ctx, "running trigger worker heartbeat timeout", map[string]interface{}{
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				"subIds":                 tWorker.GetAssignSubIds(),
			})
		}
		return
	}
	reportSubIds := tWorker.GetReportSubId()
	subIds := tWorker.GetAssignSubIds()
	for subId, t := range subIds {
		if _, exist := reportSubIds[subId]; !exist && twInfo.HeartbeatTime.Sub(t) > 2*twInfo.HeartbeatPeriod {
			//trigger worker need but report is no,need add
			err := m.AssignSubscription(ctx, *tWorker.Info, subId)
			if err != nil {
				log.Warning(ctx, "compare trigger worker subscription add subscription error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: subId,
				})
			}
		}
	}
}
