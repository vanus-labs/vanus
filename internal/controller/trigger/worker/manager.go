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

//go:generate mockgen -source=manager.go  -destination=mock_manager.go -package=worker
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/util"
)

const (
	defaultCheckInterval       = 5 * time.Second
	defaultLostHeartbeatTime   = 30 * time.Second
	defaultHeartbeatTimeout    = 60 * time.Second
	defaultDisconnectCleanTime = 120 * time.Second
	defaultWaitRunningTimeout  = 30 * time.Second
	defaultStartWorkerDuration = 10 * time.Second
)

type Manager interface {
	AddTriggerWorker(ctx context.Context, addr string) error
	GetTriggerWorker(addr string) TriggerWorker
	RemoveTriggerWorker(ctx context.Context, addr string)
	UpdateTriggerWorkerInfo(ctx context.Context, addr string) error
	GetActiveRunningTriggerWorker() []metadata.TriggerWorkerInfo
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

	StartWorkerDuration       time.Duration
	StartSubscriptionDuration time.Duration
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
		c.WaitRunningTimeout = defaultWaitRunningTimeout
	}
	if c.StartWorkerDuration <= 0 {
		c.StartWorkerDuration = defaultStartWorkerDuration
	}
}

type manager struct {
	config               Config
	triggerWorkers       map[string]TriggerWorker
	storage              storage.TriggerWorkerStorage
	subscriptionManager  subscription.Manager
	lock                 sync.RWMutex
	onRemoveSubscription OnTriggerWorkerRemoveSubscription
	ctx                  context.Context
	stop                 context.CancelFunc
}

func NewTriggerWorkerManager(config Config,
	storage storage.TriggerWorkerStorage,
	subscriptionManager subscription.Manager,
	handler OnTriggerWorkerRemoveSubscription) Manager {
	config.init()
	m := &manager{
		config:               config,
		storage:              storage,
		subscriptionManager:  subscriptionManager,
		triggerWorkers:       map[string]TriggerWorker{},
		onRemoveSubscription: handler,
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) GetTriggerWorker(addr string) TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist || tWorker.GetPhase() == metadata.TriggerWorkerPhasePaused {
		return nil
	}
	return tWorker
}

func (m *manager) AddTriggerWorker(ctx context.Context, addr string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	tWorker, exist := m.triggerWorkers[addr]
	if !exist {
		tWorker = NewTriggerWorkerByAddr(addr, m.subscriptionManager)
		if err := tWorker.Start(ctx); err != nil {
			return err
		}
		m.triggerWorkers[addr] = tWorker
	} else {
		phase := tWorker.GetPhase()
		if phase == metadata.TriggerWorkerPhasePaused {
			// wait clean
			return errors.ErrResourceAlreadyExist
		}
		log.Info(ctx, "repeat add trigger worker", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
		})
		tWorker.Reset()
	}
	err := m.storage.SaveTriggerWorker(ctx, tWorker.GetInfo())
	if err != nil {
		return err
	}
	go func(tWorker TriggerWorker) {
		time.Sleep(time.Second)
		m.startTriggerWorker(m.ctx, tWorker)
	}(tWorker)
	log.Info(ctx, "add trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
	})
	return nil
}

func (m *manager) RemoveTriggerWorker(ctx context.Context, addr string) {
	tWorker := m.GetTriggerWorker(addr)
	if tWorker == nil {
		log.Info(ctx, "remove trigger worker not exist or phase is paused", map[string]interface{}{
			log.KeyTriggerWorkerAddr: addr,
		})
		return
	}
	tWorker.SetPhase(metadata.TriggerWorkerPhasePaused)
	err := m.storage.SaveTriggerWorker(ctx, tWorker.GetInfo())
	if err != nil {
		log.Warning(ctx, "trigger worker remove save phase error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: addr,
		})
	}
	m.cleanTriggerWorker(ctx, tWorker)
}

func (m *manager) UpdateTriggerWorkerInfo(ctx context.Context, addr string) error {
	tWorker := m.GetTriggerWorker(addr)
	if tWorker == nil {
		return ErrTriggerWorkerNotFound
	}
	if tWorker.GetPhase() != metadata.TriggerWorkerPhaseRunning {
		tWorker.SetPhase(metadata.TriggerWorkerPhaseRunning)
		err := m.storage.SaveTriggerWorker(ctx, tWorker.GetInfo())
		if err != nil {
			log.Warning(ctx, "storage save trigger worker phase to running error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: addr,
			})
		} else {
			log.Info(ctx, "trigger worker phase to running", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
			})
		}
	}
	tWorker.Polish()
	return nil
}

func (m *manager) startTriggerWorker(ctx context.Context, tWorker TriggerWorker) {
	assignSubscription := tWorker.GetAssignedSubscriptions()
	err := tWorker.RemoteStart(ctx)
	if err != nil {
		log.Warning(ctx, "trigger worker start error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
		})
		return
	}
	log.Info(ctx, "trigger worker start success", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
	})
	// trigger worker restart need assign to trigger worker again
	for _, id := range assignSubscription {
		tWorker.AssignSubscription(id)
	}
}
func (m *manager) cleanTriggerWorker(ctx context.Context, tWorker TriggerWorker) {
	hasFail := m.doTriggerWorkerLeave(ctx, tWorker)
	if hasFail {
		log.Warning(ctx, "trigger worker leave remove subscription has fail", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
		})
		return
	}
	log.Info(ctx, "do trigger worker leave success", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
	})
	err := m.storage.DeleteTriggerWorker(ctx, tWorker.GetInfo().ID)
	if err != nil {
		log.Warning(ctx, "storage delete trigger worker error", map[string]interface{}{
			log.KeyError:             err,
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
		})
		return
	}
	m.deleteTriggerWorker(tWorker.GetAddr())
}

func (m *manager) doTriggerWorkerLeave(ctx context.Context, tWorker TriggerWorker) bool {
	assignSubscription := tWorker.GetAssignedSubscriptions()
	// reallocate subscription
	var hasFail bool
	for _, id := range assignSubscription {
		err := m.onRemoveSubscription(ctx, id, tWorker.GetAddr())
		if err != nil {
			hasFail = true
			log.Warning(ctx, "trigger worker leave on remove subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeySubscriptionID:    id,
				log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
			})
		}
	}
	return hasFail
}

func (m *manager) GetActiveRunningTriggerWorker() []metadata.TriggerWorkerInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()
	now := time.Now()
	runningTriggerWorker := make([]metadata.TriggerWorkerInfo, 0)
	for _, tWorker := range m.triggerWorkers {
		if !tWorker.IsActive() ||
			now.Sub(tWorker.GetHeartbeatTime()) > 10*time.Second {
			continue
		}
		runningTriggerWorker = append(runningTriggerWorker, tWorker.GetInfo())
	}
	return runningTriggerWorker
}

func (m *manager) getTriggerWorkers() []TriggerWorker {
	m.lock.RLock()
	defer m.lock.RUnlock()
	tWorkers := make([]TriggerWorker, 0)
	for _, tWorker := range m.triggerWorkers {
		tWorkers = append(tWorkers, tWorker)
	}
	return tWorkers
}

func (m *manager) deleteTriggerWorker(addr string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	log.Info(m.ctx, "remove trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: addr,
	})
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
		tWorker := newTriggerWorker(twInfo, m.subscriptionManager)
		if err = tWorker.Start(ctx); err != nil {
			return err
		}
		m.triggerWorkers[twInfo.Addr] = tWorker
	}
	subscriptions := m.subscriptionManager.ListSubscription(ctx)
	for _, metaData := range subscriptions {
		if metaData.TriggerWorker != "" {
			tWorker, exist := m.triggerWorkers[metaData.TriggerWorker]
			if exist {
				tWorker.AssignSubscription(metaData.ID)
			}
		}
	}
	return nil
}

func (m *manager) Stop() {
	m.stop()
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, tWorker := range m.triggerWorkers {
		_ = tWorker.Close()
	}
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
		go func(tWorker TriggerWorker) {
			phase := tWorker.GetPhase()
			defer wg.Done()
			switch phase {
			case metadata.TriggerWorkerPhaseRunning:
				m.runningTriggerWorkerHandler(ctx, tWorker)
			case metadata.TriggerWorkerPhasePending:
				m.pendingTriggerWorkerHandler(ctx, tWorker)
			case metadata.TriggerWorkerPhasePaused:
				m.cleanTriggerWorker(ctx, tWorker)
			case metadata.TriggerWorkerPhaseDisconnect:
				var d time.Duration
				if tWorker.IsActive() {
					d = now.Sub(tWorker.GetHeartbeatTime())
				} else {
					d = now.Sub(tWorker.GetPendingTime())
				}
				if d > m.config.DisconnectCleanTime {
					log.Info(ctx, "trigger worker disconnect timeout", map[string]interface{}{
						log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
					})
					m.cleanTriggerWorker(ctx, tWorker)
				}
			}
		}(tWorker)
	}
	wg.Wait()
	log.Debug(ctx, "trigger worker check complete", nil)
}

func (m *manager) pendingTriggerWorkerHandler(ctx context.Context, tWorker TriggerWorker) {
	now := time.Now()
	d := now.Sub(tWorker.GetPendingTime())
	if d > m.config.WaitRunningTimeout {
		tWorker.SetPhase(metadata.TriggerWorkerPhasePaused)
		log.Info(ctx, "pending trigger worker heartbeat timeout change phase to paused", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
		})
		m.cleanTriggerWorker(ctx, tWorker)
	} else if d > m.config.StartWorkerDuration {
		m.startTriggerWorker(ctx, tWorker)
	}
}

func (m *manager) runningTriggerWorkerHandler(ctx context.Context, tWorker TriggerWorker) {
	now := time.Now()
	var d time.Duration
	if tWorker.IsActive() {
		d = now.Sub(tWorker.GetHeartbeatTime())
	} else {
		d = now.Sub(tWorker.GetPendingTime())
	}
	if d > m.config.HeartbeatTimeout {
		tWorker.SetPhase(metadata.TriggerWorkerPhaseDisconnect)
		err := m.storage.SaveTriggerWorker(ctx, tWorker.GetInfo())
		if err != nil {
			log.Info(ctx, "running trigger worker heartbeat timeout save error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
			})
		} else {
			log.Info(ctx, "running trigger worker heartbeat timeout", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
			})
		}
	} else if d > m.config.LostHeartbeatTime {
		log.Warning(ctx, "trigger worker lost heartbeat", map[string]interface{}{
			log.KeyTriggerWorkerAddr: tWorker.GetAddr(),
			"duration":               d,
		})
	}
}
