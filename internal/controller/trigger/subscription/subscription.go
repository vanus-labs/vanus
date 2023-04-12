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

//go:generate mockgen -source=subscription.go -destination=mock_subscription.go -package=subscription
package subscription

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/internal/controller/trigger/metadata"
	"github.com/vanus-labs/vanus/internal/controller/trigger/secret"
	"github.com/vanus-labs/vanus/internal/controller/trigger/storage"
	"github.com/vanus-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
)

type Manager interface {
	SaveOffset(ctx context.Context, id vanus.ID, offsets info.ListOffsetInfo, commit bool) error
	// GetOrSaveOffset get offset only from etcd, if it isn't exist will get from cli and save to etcd,
	// and it contains retry eb offset
	GetOrSaveOffset(ctx context.Context, id vanus.ID) (info.ListOffsetInfo, error)
	// GetOffset get offset only from etcd, it doesn't contain retry eb offset
	GetOffset(ctx context.Context, id vanus.ID) (info.ListOffsetInfo, error)
	GetDeadLetterOffset(ctx context.Context, id vanus.ID) (uint64, error)
	SaveDeadLetterOffset(ctx context.Context, id vanus.ID, offset uint64) error
	ResetOffsetByTimestamp(ctx context.Context, id vanus.ID, timestamp uint64) (info.ListOffsetInfo, error)
	ListSubscription(ctx context.Context) []*metadata.Subscription
	GetSubscription(ctx context.Context, id vanus.ID) *metadata.Subscription
	GetSubscriptionByName(ctx context.Context, namespaceID vanus.ID, name string) *metadata.Subscription
	AddSubscription(ctx context.Context, subscription *metadata.Subscription) error
	UpdateSubscription(ctx context.Context, subscription *metadata.Subscription) error
	Heartbeat(ctx context.Context, id vanus.ID, addr string, time time.Time) error
	DeleteSubscription(ctx context.Context, id vanus.ID) error
	Init(ctx context.Context) error
	Start()
	Stop()
}

const (
	defaultCommitInterval = time.Second
)

type manager struct {
	cl              cluster.Cluster
	ebCli           api.Client
	secretStorage   secret.Storage
	storage         storage.Storage
	offsetManager   offset.Manager
	lock            sync.RWMutex
	subscriptionMap map[vanus.ID]*metadata.Subscription
	// key: eventbusID, value: deadLetterEventbusID
	deadLetterEventbusMap map[vanus.ID]vanus.ID
	// key: deadLetterEventbusID, value: eventlogID
	deadLetterEventlogMap map[vanus.ID]vanus.ID
	retryEventlogID       vanus.ID
	retryEventbusID       vanus.ID
	timerEventbusID       vanus.ID
}

func NewSubscriptionManager(storage storage.Storage, secretStorage secret.Storage,
	ebCli api.Client, cl cluster.Cluster) Manager {
	m := &manager{
		cl:                    cl,
		ebCli:                 ebCli,
		storage:               storage,
		secretStorage:         secretStorage,
		deadLetterEventbusMap: map[vanus.ID]vanus.ID{},
		deadLetterEventlogMap: map[vanus.ID]vanus.ID{},
		subscriptionMap:       map[vanus.ID]*metadata.Subscription{},
		offsetManager:         offset.NewOffsetManager(storage, defaultCommitInterval),
	}
	return m
}

func (m *manager) ListSubscription(_ context.Context) []*metadata.Subscription {
	m.lock.RLock()
	defer m.lock.RUnlock()
	list := make([]*metadata.Subscription, 0, len(m.subscriptionMap))
	for _, subscription := range m.subscriptionMap {
		list = append(list, subscription)
	}
	return list
}

func (m *manager) GetSubscriptionByName(_ context.Context, namespaceID vanus.ID, name string) *metadata.Subscription {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, sub := range m.subscriptionMap {
		if sub.NamespaceID != namespaceID {
			continue
		}
		if sub.Name == name {
			return sub
		}
	}
	return nil
}

func (m *manager) GetSubscription(_ context.Context, id vanus.ID) *metadata.Subscription {
	m.lock.RLock()
	defer m.lock.RUnlock()
	sub, exist := m.subscriptionMap[id]
	if !exist || sub.Phase == metadata.SubscriptionPhaseToDelete {
		return nil
	}
	return sub
}

func (m *manager) getSystemEventbusAndEventlog(ctx context.Context, name string) (vanus.ID, vanus.ID, error) {
	eb, err := m.cl.EventbusService().GetSystemEventbusByName(ctx, name)
	if err != nil {
		return 0, 0, err
	}
	eventbusID := vanus.NewIDFromUint64(eb.Id)
	if len(eb.Logs) != 1 {
		return 0, 0, errors.ErrInternal.WithMessage(
			fmt.Sprintf("system eventbus %s eventlog length is %d", name, len(eb.Logs)))
	}
	eventlogID := vanus.NewIDFromUint64(eb.Logs[0].EventlogId)
	return eventbusID, eventlogID, nil
}

func (m *manager) initDeadLetterEventbus(ctx context.Context, eventbusID vanus.ID) error {
	_, ok := m.deadLetterEventbusMap[eventbusID]
	if ok {
		return nil
	}
	deadLetterEventbusName := primitive.GetDeadLetterEventbusName(eventbusID)
	// TODO dlq eb belongs to system?
	deadLetterEventbusID, deadLetterEventlogID, err := m.getSystemEventbusAndEventlog(ctx, deadLetterEventbusName)
	if err != nil {
		return err
	}
	m.deadLetterEventbusMap[eventbusID] = deadLetterEventbusID
	m.deadLetterEventlogMap[deadLetterEventbusID] = deadLetterEventlogID
	return nil
}

func (m *manager) initRetryEventbus(ctx context.Context) error {
	retryEventbusID, retryEventlogID, err := m.getSystemEventbusAndEventlog(ctx, primitive.RetryEventbusName)
	if err != nil {
		return err
	}
	m.retryEventbusID = retryEventbusID
	m.retryEventlogID = retryEventlogID
	return nil
}

func (m *manager) initTimerEventbus(ctx context.Context) error {
	eb, err := m.cl.EventbusService().GetSystemEventbusByName(ctx, primitive.TimerEventbusName)
	if err != nil {
		return err
	}
	m.timerEventbusID = vanus.NewIDFromUint64(eb.Id)
	return nil
}

func (m *manager) AddSubscription(ctx context.Context, subscription *metadata.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.retryEventbusID == 0 {
		err := m.initRetryEventbus(ctx)
		if err != nil {
			return err
		}
	}
	if m.timerEventbusID == 0 {
		err := m.initTimerEventbus(ctx)
		if err != nil {
			return err
		}
	}
	err := m.initDeadLetterEventbus(ctx, subscription.EventbusID)
	if err != nil {
		return err
	}
	subscription.DeadLetterEventbusID = m.deadLetterEventbusMap[subscription.EventbusID]
	subscription.RetryEventbusID = m.retryEventbusID
	subscription.TimerEventbusID = m.timerEventbusID
	if subscription.SinkCredential != nil {
		if err := m.secretStorage.Write(ctx, subscription.ID, subscription.SinkCredential); err != nil {
			return err
		}
	}
	if err := m.storage.CreateSubscription(ctx, subscription); err != nil {
		return err
	}
	m.subscriptionMap[subscription.ID] = subscription
	metrics.SubscriptionGauge.WithLabelValues(subscription.EventbusID.Key()).Inc()
	if subscription.Transformer.Exist() {
		metrics.SubscriptionTransformerGauge.WithLabelValues(subscription.EventbusID.Key()).Inc()
	}
	return nil
}

func (m *manager) UpdateSubscription(ctx context.Context, sub *metadata.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	curr, exist := m.subscriptionMap[sub.ID]
	if !exist {
		return nil
	}
	if err := m.storage.UpdateSubscription(ctx, sub); err != nil {
		return err
	}
	m.subscriptionMap[sub.ID] = sub
	if reflect.DeepEqual(curr.SinkCredential, sub.SinkCredential) {
		return nil
	}
	if sub.SinkCredential == nil {
		if err := m.secretStorage.Delete(ctx, sub.ID); err != nil {
			return err
		}
	} else {
		if err := m.secretStorage.Write(ctx, sub.ID, sub.SinkCredential); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSubscription will do
// 1.delete offset
// 2.delete subscription .
func (m *manager) DeleteSubscription(ctx context.Context, id vanus.ID) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	subscription, exist := m.subscriptionMap[id]
	if !exist {
		return nil
	}
	if err := m.offsetManager.RemoveRegisterSubscription(ctx, id); err != nil {
		return err
	}
	if err := m.storage.DeleteSubscription(ctx, id); err != nil {
		return err
	}
	if err := m.secretStorage.Delete(ctx, id); err != nil {
		return err
	}
	delete(m.subscriptionMap, id)
	metrics.SubscriptionGauge.WithLabelValues(subscription.EventbusID.Key()).Dec()
	if subscription.Transformer.Exist() {
		metrics.SubscriptionTransformerGauge.WithLabelValues(subscription.EventbusID.Key()).Dec()
	}
	return nil
}

func (m *manager) Heartbeat(ctx context.Context, id vanus.ID, addr string, time time.Time) error {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return errors.ErrResourceNotFound
	}
	if subscription.TriggerWorker != addr {
		// data is not consistent, record
		log.Error(ctx).
			Stringer(log.KeySubscriptionID, id).
			Str(log.KeyTriggerWorkerAddr, subscription.TriggerWorker).
			Str("running_addr", addr).
			Msg("subscription trigger worker invalid")
	}
	subscription.HeartbeatTime = time
	return nil
}

func (m *manager) Stop() {
	m.offsetManager.Stop()
}

func (m *manager) Init(ctx context.Context) error {
	subList, err := m.storage.ListSubscription(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx).Int("size", len(subList)).Msg("subscription size")
	for i := range subList {
		sub := subList[i]
		if sub.SinkCredentialType != nil {
			credential, err := m.secretStorage.Read(ctx, sub.ID, *sub.SinkCredentialType)
			if err != nil {
				return err
			}
			sub.SinkCredential = credential
		}
		m.subscriptionMap[sub.ID] = sub
		metrics.SubscriptionGauge.WithLabelValues(sub.EventbusID.Key()).Inc()
		if sub.Transformer.Exist() {
			metrics.SubscriptionTransformerGauge.WithLabelValues(sub.EventbusID.Key()).Inc()
		}
		if sub.TriggerWorker != "" {
			metrics.CtrlTriggerGauge.WithLabelValues(sub.TriggerWorker).Inc()
		}
	}
	return nil
}

func (m *manager) Start() {
	m.offsetManager.Start()
}
