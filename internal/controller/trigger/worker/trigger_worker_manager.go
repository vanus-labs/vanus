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
	"fmt"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

const (
	defaultCheckInterval       = 5 * time.Second
	defaultLostHeartbeatTime   = 30 * time.Second
	defaultHeartbeatTimeout    = 60 * time.Second
	defaultDisconnectCleanTime = 120 * time.Second
)

type Manager interface {
	AssignSubscription(ctx context.Context, tWorker *TriggerWorker, subId vanus.ID)
	UnAssignSubscription(ctx context.Context, addr string, subId vanus.ID) error
	AddTriggerWorker(ctx context.Context, addr string) error
	GetTriggerWorker(ctx context.Context, addr string) *TriggerWorker
	RemoveTriggerWorker(ctx context.Context, addr string)
	UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[vanus.ID]struct{}) error
	GetActiveRunningTriggerWorker() []info.TriggerWorkerInfo
	Init(ctx context.Context) error
	Start()
	Stop()
}

var (
	ErrTriggerWorkerNotFound = fmt.Errorf("trigger worker not found")
)

type OnTriggerWorkerRemoveSubscription func(ctx context.Context, subId vanus.ID, addr string) error

type Config struct {
	CheckInterval       time.Duration
	LostHeartbeatTime   time.Duration
	HeartbeatTimeout    time.Duration
	DisconnectCleanTime time.Duration
	WaitRunningTimeout  time.Duration
}

func (c *Config) init() {
	if c.CheckInterval <= 0 {
		c.CheckInterval = defaultCheckInterval
	}
	if c.LostHeartbeatTime <= 0 {
		c.LostHeartbeatTime = defaultLostHeartbeatTime
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if c.DisconnectCleanTime <= 0 {
		c.DisconnectCleanTime = defaultDisconnectCleanTime
	}
	if c.WaitRunningTimeout <= 0 {
		c.WaitRunningTimeout = 30 * time.Second
	}
}

type manager struct {
	config               Config
	triggerWorkers       map[string]*TriggerWorker
	storage              storage.TriggerWorkerStorage
	subscriptionManager  subscription.Manager
	lock                 sync.RWMutex
	onRemoveSubscription OnTriggerWorkerRemoveSubscription
	ctx                  context.Context
	stop                 context.CancelFunc
}

func NewTriggerWorkerManager(config Config, storage storage.TriggerWorkerStorage, subscriptionManager subscription.Manager, handler OnTriggerWorkerRemoveSubscription) Manager {
	config.init()
	m := &manager{
		config:               config,
		storage:              storage,
		subscriptionManager:  subscriptionManager,
		triggerWorkers:       map[string]*TriggerWorker{},
		onRemoveSubscription: handler,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) AssignSubscription(ctx context.Context, tWorker *TriggerWorker, subId vanus.ID) {
	tWorker.AddAssignSub(subId)
	log.Info(ctx, "trigger worker assign a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetAssignSubIds(),
	})
	err := m.startSubscription(ctx, tWorker, subId)
	if err != nil {
		//wait check start again
		log.Warning(ctx, "assign subscription but start subscription error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			log.KeySubscriptionID:    subId,
		})
	} else {
		log.Info(ctx, "assign a subscription start subscription success", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			log.KeySubscriptionID:    subId,
		})
	}
}

func (m *manager) startSubscription(ctx context.Context, tWorker *TriggerWorker, subId vanus.ID) error {
	sub, err := m.subscriptionManager.GetSubscription(ctx, subId)
	if err != nil {
		return err
	}
	err = tWorker.AddSubscription(ctx, sub)
	if err != nil {
		return err
	}
	return nil
}

func (m *manager) UnAssignSubscription(ctx context.Context, addr string, subId vanus.ID) error {
	tWorker := m.GetTriggerWorker(ctx, addr)
	if tWorker == nil {
		return nil
	}
	if tWorker.GetPhase() == info.TriggerWorkerPhaseRunning {
		err := tWorker.RemoveSubscriptions(ctx, subId)
		if err != nil {
			return err
		}
	}
	tWorker.RemoveAssignSub(subId)
	log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.info.Addr,
		log.KeySubscriptionID:    subId,
		"subIds":                 tWorker.GetAssignSubIds(),
	})
	return nil
}

func (m *manager) GetTriggerWorker(ctx context.Context, addr string) *TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist || tWorker.GetPhase() == info.TriggerWorkerPhasePaused {
		return nil
	}
	return tWorker
}

func (m *manager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	m.lock.Unlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		tWorker = NewTriggerWorker(info.NewTriggerWorkerInfo(addr))
	} else {
		phase := tWorker.GetPhase()
		if phase == info.TriggerWorkerPhasePaused {
			//wait clean
			return errors.ErrResourceAlreadyExist
		}
		log.Info(ctx, "repeat add trigger worker", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			"subIds":                 tWorker.GetAssignSubIds(),
		})
		tWorker.ResetReportSubId()
		if phase == info.TriggerWorkerPhasePending {
			return nil
		}
	}
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.info)
	if err != nil {
		return err
	}
	if !exist {
		m.triggerWorkers[addr] = tWorker
	}
	log.Info(ctx, "add trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.info.Addr,
	})
	return nil
}

func (m *manager) RemoveTriggerWorker(ctx context.Context, addr string) {
	tWorker := m.GetTriggerWorker(ctx, addr)
	if tWorker == nil {
		log.Info(ctx, "remove trigger worker not exist or phase is paused", map[string]interface{}{
			log.KeyTriggerWorkerAddr: addr,
		})
		return
	}
	tWorker.SetPhase(info.TriggerWorkerPhasePaused)
	err := m.storage.SaveTriggerWorker(ctx, *tWorker.info)
	if err != nil {
		log.Warning(ctx, "trigger worker remove save phase error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: addr,
			"assign_subscription":    tWorker.GetAssignSubIds(),
		})
	}
	m.cleanTriggerWorker(ctx, tWorker)
}

func (m *manager) UpdateTriggerWorkerInfo(ctx context.Context, addr string, subIds map[vanus.ID]struct{}) error {
	tWorker := m.GetTriggerWorker(ctx, addr)
	if tWorker == nil {
		return ErrTriggerWorkerNotFound
	}
	if tWorker.GetPhase() != info.TriggerWorkerPhaseRunning {
		tWorker.SetPhase(info.TriggerWorkerPhaseRunning)
		err := m.storage.SaveTriggerWorker(ctx, *tWorker.info)
		if err != nil {
			log.Warning(ctx, "storage save trigger worker phase to running error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: addr,
			})
		} else {
			log.Info(ctx, "trigger worker phase to running", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			})
		}
	}
	tWorker.SetReportSubId(subIds)
	return nil
}

func (m *manager) cleanTriggerWorker(ctx context.Context, tWorker *TriggerWorker) {
	hasFail := m.doTriggerWorkerLeave(ctx, tWorker)
	if hasFail {
		log.Warning(ctx, "trigger worker leave remove subscription has fail", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			"subIds":                 tWorker.GetAssignSubIds(),
		})
		return
	}
	err := m.storage.DeleteTriggerWorker(ctx, tWorker.info.Id)
	if err != nil {
		log.Warning(ctx, "storage delete trigger worker error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.info.Addr,
		})
		return
	}
	m.deleteTriggerWorker(tWorker.info.Addr)
}

func (m *manager) doTriggerWorkerLeave(ctx context.Context, tWorker *TriggerWorker) bool {
	subIds := tWorker.GetAssignSubIds()
	//reallocate subscription
	var hasFail bool
	for subId := range subIds {
		err := m.onRemoveSubscription(ctx, subId, tWorker.info.Addr)
		if err != nil {
			hasFail = true
			log.Info(ctx, "trigger worker leave on remove subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeySubscriptionID:    subId,
				log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			})
		} else {
			tWorker.RemoveAssignSub(subId)
			log.Info(ctx, "trigger worker remove a subscription", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.info.Addr,
				log.KeySubscriptionID:    subId,
				"subIds":                 tWorker.GetAssignSubIds(),
			})
		}
	}
	return hasFail
}

func (m *manager) GetActiveRunningTriggerWorker() []info.TriggerWorkerInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()
	now := time.Now()
	var runningTriggerWorker []info.TriggerWorkerInfo
	for _, tWorker := range m.triggerWorkers {
		if tWorker.GetPhase() != info.TriggerWorkerPhaseRunning || tWorker.info.HeartbeatTime == nil || now.Sub(tWorker.GetLastHeartbeatTime()) > 10*time.Second {
			continue
		}
		runningTriggerWorker = append(runningTriggerWorker, *tWorker.info)
	}
	return runningTriggerWorker
}

func (m *manager) getTriggerWorkers() []*TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var tWorkers []*TriggerWorker
	for _, tWorker := range m.triggerWorkers {
		tWorkers = append(tWorkers, tWorker)
	}
	return tWorkers
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
			}
		}
	}
	return nil
}

func (m *manager) Stop() {
	m.stop()
}

func (m *manager) Start() {
	go util.UntilWithContext(m.ctx, m.check, m.config.CheckInterval)
}

func (m *manager) check(ctx context.Context) {
	log.Debug(ctx, "trigger worker check begin", nil)
	var wg sync.WaitGroup
	now := time.Now()
	workers := m.getTriggerWorkers()
	for _, tWorker := range workers {
		wg.Add(1)
		go func(tWorker *TriggerWorker) {
			phase := tWorker.GetPhase()
			defer wg.Done()
			switch phase {
			case info.TriggerWorkerPhaseRunning:
				m.runningTriggerWorkerHandler(ctx, tWorker)
			case info.TriggerWorkerPhasePending:
				m.pendingTriggerWorkerHandler(ctx, tWorker)
			case info.TriggerWorkerPhasePaused:
				m.cleanTriggerWorker(ctx, tWorker)
			case info.TriggerWorkerPhaseDisconnect:
				if now.Sub(tWorker.GetLastHeartbeatTime()) > m.config.DisconnectCleanTime {
					log.Info(ctx, "trigger worker disconnect timeout", map[string]interface{}{
						log.KeyTriggerWorkerAddr: tWorker.info.Addr,
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
	twInfo := tWorker.info
	if now.Sub(twInfo.PendingTime) > m.config.WaitRunningTimeout {
		tWorker.SetPhase(info.TriggerWorkerPhasePaused)
		log.Info(ctx, "pending trigger worker heartbeat timeout change phase to paused", map[string]interface{}{
			"triggerWorker": twInfo,
		})
		m.cleanTriggerWorker(ctx, tWorker)
	} else {
		err := tWorker.Start(ctx)
		if err != nil {
			log.Warning(ctx, "trigger worker start error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			})
		} else {
			log.Info(ctx, "trigger worker start success", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.info.Addr,
			})
		}
	}
}

func (m *manager) runningTriggerWorkerHandler(ctx context.Context, tWorker *TriggerWorker) {
	now := time.Now()
	twInfo := tWorker.info
	assignSub := tWorker.GetAssignSubIds()
	d := now.Sub(tWorker.GetLastHeartbeatTime())
	if d > m.config.HeartbeatTimeout {
		tWorker.SetPhase(info.TriggerWorkerPhaseDisconnect)
		err := m.storage.SaveTriggerWorker(ctx, *twInfo)
		if err != nil {
			log.Info(ctx, "running trigger worker heartbeat timeout save error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				"assign_subscription":    assignSub,
			})
		} else {
			log.Info(ctx, "running trigger worker heartbeat timeout", map[string]interface{}{
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				"assign_subscription":    assignSub,
			})
		}
		return
	}
	if d > m.config.LostHeartbeatTime {
		log.Warning(ctx, "trigger worker lost heartbeat", map[string]interface{}{
			log.KeyTriggerWorkerAddr: twInfo.Addr,
			"assign_subscription":    assignSub,
			"duration":               d,
		})
		return
	}
	if tWorker.info.HeartbeatTime == nil {
		return
	}

	reportSubIds := tWorker.GetReportSubId()
	for subId := range reportSubIds {
		m.subscriptionManager.Heartbeat(ctx, subId, twInfo.Addr, tWorker.GetLastHeartbeatTime())
	}
	for subId, t := range assignSub {
		if _, exist := reportSubIds[subId]; exist {
			continue
		}
		if tWorker.GetLastHeartbeatTime().Sub(t) < 15*time.Second {
			continue
		}
		//trigger worker assign but report is no, need start
		err := m.startSubscription(ctx, tWorker, subId)
		if err != nil {
			if err == subscription.ErrSubscriptionNotExist {
				log.Info(ctx, "check trigger worker assign subscription not exist,remove assign subscription", map[string]interface{}{
					log.KeyTriggerWorkerAddr: twInfo.Addr,
					log.KeySubscriptionID:    subId,
				})
				tWorker.RemoveAssignSub(subId)
				continue
			}
			log.Warning(ctx, "check trigger worker start subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				log.KeySubscriptionID:    subId,
			})
		} else {
			log.Info(ctx, "check trigger worker start subscription success", map[string]interface{}{
				log.KeyTriggerWorkerAddr: twInfo.Addr,
				log.KeySubscriptionID:    subId,
			})
		}
	}
}
