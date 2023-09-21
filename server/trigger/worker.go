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

//go:generate mockgen -source=worker.go -destination=mock_worker.go -package=trigger
package trigger

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/vanus-labs/vanus/api/cluster"
	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/api/errors"
	metapb "github.com/vanus-labs/vanus/api/meta"
	vanus "github.com/vanus-labs/vanus/api/vsr"
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/metrics"

	"github.com/vanus-labs/vanus/pkg/convert"
	"github.com/vanus-labs/vanus/server/trigger/trigger"
)

type Worker interface {
	Init(ctx context.Context) error
	Register(ctx context.Context) error
	Unregister(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	AddSubscription(ctx context.Context, subscription *primitive.Subscription) error
	RemoveSubscription(ctx context.Context, id vanus.ID) error
	PauseSubscription(ctx context.Context, id vanus.ID) error
	StartSubscription(ctx context.Context, id vanus.ID) error
}

const (
	defaultHeartbeatInterval = 2 * time.Second
)

type newTrigger func(subscription *primitive.Subscription, options ...trigger.Option) (trigger.Trigger, error)

type worker struct {
	triggerMap map[vanus.ID]trigger.Trigger
	ctx        context.Context
	stop       context.CancelFunc
	config     Config
	newTrigger newTrigger
	wg         sync.WaitGroup
	lock       sync.RWMutex
	tgLock     sync.RWMutex
	client     ctrlpb.TriggerControllerClient
	ctrl       cluster.Cluster
}

func NewWorker(config Config) Worker {
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaultHeartbeatInterval
	}

	m := &worker{
		config:     config,
		ctrl:       cluster.NewClusterController(config.ControllerAddr, insecure.NewCredentials()),
		triggerMap: make(map[vanus.ID]trigger.Trigger),
		newTrigger: trigger.NewTrigger,
	}
	m.client = m.ctrl.TriggerService().RawClient()
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (w *worker) getTrigger(id vanus.ID) (trigger.Trigger, bool) {
	w.tgLock.RLock()
	defer w.tgLock.RUnlock()
	t, exist := w.triggerMap[id]
	return t, exist
}

func (w *worker) addTrigger(id vanus.ID, t trigger.Trigger) {
	w.tgLock.Lock()
	defer w.tgLock.Unlock()
	w.triggerMap[id] = t
}

func (w *worker) deleteTrigger(id vanus.ID) {
	w.tgLock.Lock()
	defer w.tgLock.Unlock()
	delete(w.triggerMap, id)
}

func (w *worker) Init(_ context.Context) error {
	err := w.ctrl.WaitForControllerReady(false)
	return err
}

func (w *worker) Register(ctx context.Context) error {
	_, err := w.client.RegisterTriggerWorker(ctx, &ctrlpb.RegisterTriggerWorkerRequest{
		Address: w.config.TriggerAddr,
	})
	return err
}

func (w *worker) Unregister(ctx context.Context) error {
	_, err := w.client.UnregisterTriggerWorker(ctx, &ctrlpb.UnregisterTriggerWorkerRequest{
		Address: w.config.TriggerAddr,
	})
	return err
}

func (w *worker) Start(_ context.Context) error {
	return w.startHeartbeat(w.ctx)
}

func (w *worker) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	// stop subscription
	for id, t := range w.triggerMap {
		wg.Add(1)
		go func(id vanus.ID, t trigger.Trigger) {
			defer wg.Done()
			_ = t.Stop(ctx)
		}(id, t)
	}

	wg.Wait()
	// commit offset
	err := w.commitOffsets(ctx)
	if err != nil {
		log.Error(ctx).Err(err).Msg("commit offsets error")
	}
	// stop heartbeat
	w.stop()
	// clean trigger
	for id := range w.triggerMap {
		delete(w.triggerMap, id)
	}
	w.wg.Wait()
	if closer, ok := w.client.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

func (w *worker) AddSubscription(ctx context.Context, subscription *primitive.Subscription) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	t, exist := w.getTrigger(subscription.ID)
	if exist {
		err := t.Change(ctx, subscription)
		return err
	}
	t, err := w.newTrigger(subscription, w.getTriggerOptions(subscription)...)
	if err != nil {
		return err
	}
	err = t.Init(ctx)
	if err != nil {
		return err
	}
	err = t.Start(w.ctx)
	if err != nil {
		return err
	}
	w.addTrigger(subscription.ID, t)
	metrics.TriggerGauge.WithLabelValues(w.config.IP).Inc()
	return nil
}

func (w *worker) RemoveSubscription(ctx context.Context, id vanus.ID) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	_ = w.stopSubscription(ctx, id)
	w.deleteTrigger(id)
	metrics.TriggerGauge.WithLabelValues(w.config.IP).Dec()
	return nil
}

func (w *worker) PauseSubscription(ctx context.Context, id vanus.ID) error {
	return w.stopSubscription(ctx, id)
}

func (w *worker) StartSubscription(ctx context.Context, id vanus.ID) error {
	return w.startSubscription(ctx, id)
}

func (w *worker) startHeartbeat(ctx context.Context) error {
	w.wg.Add(1)
	defer w.wg.Done()
	f := func() interface{} {
		return &ctrlpb.TriggerWorkerHeartbeatRequest{
			Address:          w.config.TriggerAddr,
			SubscriptionInfo: w.getAllSubscriptionInfo(ctx),
		}
	}
	return w.ctrl.TriggerService().RegisterHeartbeat(ctx, w.config.HeartbeatInterval, f)
}

func (w *worker) stopSubscription(ctx context.Context, id vanus.ID) error {
	t, exist := w.getTrigger(id)
	if !exist {
		return nil
	}
	return t.Stop(ctx)
}

func (w *worker) startSubscription(ctx context.Context, id vanus.ID) error {
	t, exist := w.getTrigger(id)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	err := t.Init(ctx)
	if err != nil {
		return err
	}
	return t.Start(ctx)
}

func (w *worker) commitOffsets(ctx context.Context) error {
	_, err := w.client.CommitOffset(ctx, &ctrlpb.CommitOffsetRequest{
		ForceCommit:      true,
		SubscriptionInfo: w.getAllSubscriptionInfo(ctx),
	})
	return err
}

func (w *worker) getAllSubscriptionInfo(ctx context.Context) []*metapb.SubscriptionInfo {
	w.tgLock.RLock()
	defer w.tgLock.RUnlock()
	subInfos := make([]*metapb.SubscriptionInfo, 0, len(w.triggerMap))
	for id, t := range w.triggerMap {
		subInfos = append(subInfos, &metapb.SubscriptionInfo{
			SubscriptionId: uint64(id),
			Offsets:        convert.ToPbOffsetInfos(t.GetOffsets(ctx)),
		})
	}
	return subInfos
}

func (w *worker) getTriggerOptions(subscription *primitive.Subscription) []trigger.Option {
	opts := []trigger.Option{trigger.WithControllers(w.config.ControllerAddr)}
	config := subscription.Config
	opts = append(opts, trigger.WithRateLimit(config.RateLimit),
		trigger.WithDeliveryTimeout(config.DeliveryTimeout),
		trigger.WithMaxRetryAttempts(config.GetMaxRetryAttempts()),
		trigger.WithDisableDeadLetter(config.DisableDeadLetter),
		trigger.WithOrdered(config.OrderedEvent),
		trigger.WithGoroutineSize(w.config.SendEventGoroutineSize),
		trigger.WithSendBatchSize(w.config.SendEventBatchSize),
		trigger.WithPullBatchSize(w.config.PullEventBatchSize),
		trigger.WithMaxUACKNumber(w.config.MaxUACKEventNumber))
	return opts
}
