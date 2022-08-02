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

//go:generate mockgen -source=worker.go  -destination=mock_worker.go -package=trigger
package trigger

import (
	"context"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/trigger/errors"

	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
)

type Worker interface {
	Register(ctx context.Context) error
	UnRegister(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	AddSubscription(ctx context.Context, subscription *primitive.Subscription) error
	RemoveSubscription(ctx context.Context, id vanus.ID) error
	PauseSubscription(ctx context.Context, id vanus.ID) error
	StartSubscription(ctx context.Context, id vanus.ID) error
	ResetOffsetToTimestamp(ctx context.Context, id vanus.ID, timestamp int64) error
}

const (
	defaultCleanSubscriptionTimeout = 5 * time.Second
	defaultHeartbeatPeriod          = 2 * time.Second
)

type newSubscriptionWorker func(subscription *primitive.Subscription,
	subscriptionOffset *offset.SubscriptionOffset,
	config Config) SubscriptionWorker

type worker struct {
	subscriptionMap       sync.Map
	offsetManager         *offset.Manager
	ctx                   context.Context
	stop                  context.CancelFunc
	config                Config
	newSubscriptionWorker newSubscriptionWorker
	wg                    sync.WaitGroup
	client                *ctrlClient
}

func NewWorker(config Config) Worker {
	if config.CleanSubscriptionTimeout == 0 {
		config.CleanSubscriptionTimeout = defaultCleanSubscriptionTimeout
	}
	if config.HeartbeatPeriod == 0 {
		config.HeartbeatPeriod = defaultHeartbeatPeriod
	}
	m := &worker{
		config:                config,
		client:                NewClient(config.ControllerAddr),
		newSubscriptionWorker: NewSubscriptionWorker,
		offsetManager:         offset.NewOffsetManager(),
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (w *worker) getSubscriptionWorker(id vanus.ID) SubscriptionWorker {
	v, exist := w.subscriptionMap.Load(id)
	if !exist {
		return nil
	}
	sub, _ := v.(SubscriptionWorker)
	return sub
}

func (w *worker) Register(ctx context.Context) error {
	_, err := w.client.registerTriggerWorker(ctx, &ctrlpb.RegisterTriggerWorkerRequest{
		Address: w.config.TriggerAddr,
	})
	return err
}

func (w *worker) UnRegister(ctx context.Context) error {
	_, err := w.client.unregisterTriggerWorker(ctx, &ctrlpb.UnregisterTriggerWorkerRequest{
		Address: w.config.TriggerAddr,
	})
	return err
}

func (w *worker) Start(ctx context.Context) error {
	go w.startHeartbeat(w.ctx)
	return nil
}

func (w *worker) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	// stop subscription
	w.subscriptionMap.Range(func(key, value interface{}) bool {
		wg.Add(1)
		id, _ := key.(vanus.ID)
		go func(id vanus.ID) {
			defer wg.Done()
			w.stopSubscription(ctx, id)
		}(id)
		return true
	})
	wg.Wait()
	// commit offset
	err := w.commitOffsets(ctx)
	if err != nil {
		log.Error(ctx, "commit offsets error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	// stop heartbeat
	w.stop()
	w.subscriptionMap.Range(func(key, value interface{}) bool {
		w.subscriptionMap.Delete(key)
		return true
	})
	w.wg.Wait()
	return nil
}

func (w *worker) AddSubscription(ctx context.Context, subscription *primitive.Subscription) error {
	data, exist := w.subscriptionMap.Load(subscription.ID)
	if exist {
		sub, _ := data.(SubscriptionWorker)
		err := sub.Change(ctx, subscription)
		return err
	}
	subOffset := w.offsetManager.RegisterSubscription(subscription.ID)
	sub := w.newSubscriptionWorker(subscription, subOffset, w.config)
	w.subscriptionMap.Store(subscription.ID, sub)
	err := sub.Run(w.ctx)
	if err != nil {
		w.subscriptionMap.Delete(subscription.ID)
		w.offsetManager.RemoveSubscription(subscription.ID)
		return err
	}
	return nil
}

func (w *worker) RemoveSubscription(ctx context.Context, id vanus.ID) error {
	w.stopSubscription(ctx, id)
	w.cleanSubscription(w.ctx, id)
	return nil
}

func (w *worker) PauseSubscription(ctx context.Context, id vanus.ID) error {
	w.stopSubscription(ctx, id)
	return nil
}

func (w *worker) StartSubscription(ctx context.Context, id vanus.ID) error {
	return w.startSubscription(ctx, id)
}

func (w *worker) ResetOffsetToTimestamp(ctx context.Context,
	id vanus.ID,
	timestamp int64) error {
	v, exist := w.subscriptionMap.Load(id)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	subWorker, _ := v.(SubscriptionWorker)
	// pause subscription
	w.stopSubscription(ctx, id)
	// clean current offset
	subOffset := w.offsetManager.GetSubscription(id)
	if subOffset != nil {
		subOffset.Clear()
	}
	// reset offset
	offsets, err := subWorker.ResetOffsetToTimestamp(ctx, timestamp)
	if err != nil {
		return err
	}
	// commit offset
	err = w.commitOffset(ctx, id, offsets)
	if err != nil {
		return err
	}
	// start subscription
	err = w.startSubscription(ctx, id)
	if err != nil {
		// todo process start fail
		return err
	}
	return nil
}

func (w *worker) startHeartbeat(ctx context.Context) {
	w.wg.Add(1)
	defer w.wg.Done()
	ticker := time.NewTicker(w.config.HeartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.client.closeHeartBeat(ctx)
			return
		case <-ticker.C:
			err := w.client.heartbeat(ctx, &ctrlpb.TriggerWorkerHeartbeatRequest{
				Address:          w.config.TriggerAddr,
				SubscriptionInfo: w.getAllSubscriptionInfo(),
			})
			if err != nil {
				log.Warning(ctx, "heartbeat error", map[string]interface{}{
					log.KeyError: err,
				})
			}
		}
	}
}

func (w *worker) stopSubscription(ctx context.Context, id vanus.ID) {
	v, exist := w.subscriptionMap.Load(id)
	if !exist {
		return
	}
	subWorker, _ := v.(SubscriptionWorker)
	subWorker.Stop(ctx)
	log.Info(ctx, "stop subscription success", map[string]interface{}{
		log.KeySubscriptionID: id,
	})
}

func (w *worker) startSubscription(ctx context.Context, id vanus.ID) error {
	v, exist := w.subscriptionMap.Load(id)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	subWorker, _ := v.(SubscriptionWorker)
	err := subWorker.Run(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "start subscription success", map[string]interface{}{
		log.KeySubscriptionID: id,
	})
	return nil
}

func (w *worker) cleanSubscription(ctx context.Context, id vanus.ID) {
	w.subscriptionMap.Delete(id)
	w.offsetManager.RemoveSubscription(id)
}

func (w *worker) commitOffset(ctx context.Context, id vanus.ID, offsets info.ListOffsetInfo) error {
	return w.client.commitOffset(ctx, &ctrlpb.CommitOffsetRequest{
		ForceCommit: true,
		SubscriptionInfo: []*metapb.SubscriptionInfo{convert.ToPbSubscriptionInfo(info.SubscriptionInfo{
			SubscriptionID: id,
			Offsets:        offsets,
		})},
	})
}

func (w *worker) commitOffsets(ctx context.Context) error {
	return w.client.commitOffset(ctx, &ctrlpb.CommitOffsetRequest{
		ForceCommit:      true,
		SubscriptionInfo: w.getAllSubscriptionInfo(),
	})
}

func (w *worker) getAllSubscriptionInfo() []*metapb.SubscriptionInfo {
	var subInfos []*metapb.SubscriptionInfo
	w.subscriptionMap.Range(func(key, value interface{}) bool {
		id, _ := key.(vanus.ID)
		subOffset := w.offsetManager.GetSubscription(id)
		if subOffset == nil {
			return true
		}
		subInfos = append(subInfos, &metapb.SubscriptionInfo{
			SubscriptionId: uint64(id),
			Offsets:        convert.ToPbOffsetInfos(subOffset.GetCommit()),
		})
		return true
	})
	return subInfos
}
