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

//go:generate mockgen -source=worker.go  -destination=mock_worker.go -package=worker
package worker

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	"github.com/linkall-labs/vanus/internal/trigger/trigger"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	eventBufferSize = 2048
)

type SubscriptionWorker interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context)
	IsStart() bool
	GetStopTime() time.Time
	Change(ctx context.Context, subscription *primitive.Subscription) error
}

type subscriptionWorker struct {
	subscription *primitive.Subscription
	trigger      *trigger.Trigger
	events       chan info.EventOffset
	reader       reader.Reader
	stopTime     time.Time
	startTime    time.Time
}

func NewSubscriptionWorker(subscription *primitive.Subscription,
	subscriptionOffset *offset.SubscriptionOffset,
	config Config) SubscriptionWorker {
	sw := &subscriptionWorker{
		events:       make(chan info.EventOffset, eventBufferSize),
		subscription: subscription,
	}
	offsetMap := make(map[vanus.ID]uint64)
	for _, o := range subscription.Offsets {
		offsetMap[o.EventLogID] = o.Offset
	}
	sw.reader = reader.NewReader(getReaderConfig(subscription, config.Controllers), offsetMap, sw.events)
	sw.trigger = trigger.NewTrigger(subscription, subscriptionOffset, getTriggerOptions(config, subscription)...)
	return sw
}

func getTriggerOptions(cfg Config, subscription *primitive.Subscription) []trigger.Option {
	opts := make([]trigger.Option, 0)
	rateLimit := cfg.RateLimit
	config := subscription.Config
	if config.RateLimit != 0 {
		rateLimit = config.RateLimit
	}
	opts = append(opts, trigger.WithRateLimit(rateLimit))
	return opts
}

func (w *subscriptionWorker) Change(ctx context.Context, subscription *primitive.Subscription) error {
	if w.subscription.Sink != subscription.Sink {
		w.subscription.Sink = subscription.Sink
		err := w.trigger.ChangeTarget(subscription.Sink)
		if err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(w.subscription.Filters, subscription.Filters) {
		w.subscription.Filters = subscription.Filters
		w.trigger.ChangeFilter(subscription.Filters)
	}
	if !reflect.DeepEqual(w.subscription.InputTransformer, subscription.InputTransformer) {
		w.subscription.InputTransformer = subscription.InputTransformer
		w.trigger.ChangeInputTransformer(subscription.InputTransformer)
	}
	if !reflect.DeepEqual(w.subscription.Config, subscription.Config) {
		w.subscription.Config = subscription.Config
		w.trigger.ChangeConfig(subscription.Config)
	}
	return nil
}

func (w *subscriptionWorker) IsStart() bool {
	return !w.startTime.IsZero()
}

func (w *subscriptionWorker) GetStopTime() time.Time {
	return w.stopTime
}

func (w *subscriptionWorker) Run(ctx context.Context) error {
	err := w.reader.Start()
	if err != nil {
		return err
	}
	err = w.trigger.Start()
	if err != nil {
		w.reader.Close()
		return err
	}
	go func() {
		for event := range w.events {
			_ = w.trigger.EventArrived(ctx, info.EventRecord{EventOffset: event})
		}
	}()
	now := time.Now()
	w.startTime = now
	return nil
}

func (w *subscriptionWorker) Stop(ctx context.Context) {
	if !w.IsStart() {
		return
	}
	w.reader.Close()
	log.Info(ctx, "stop reader success", map[string]interface{}{
		log.KeySubscriptionID: w.subscription.ID,
	})
	close(w.events)
	w.trigger.Stop()
	w.stopTime = time.Now()
	log.Info(ctx, "stop trigger success", map[string]interface{}{
		log.KeySubscriptionID: w.subscription.ID,
	})
}

func getReaderConfig(sub *primitive.Subscription, controllers []string) reader.Config {
	ebVrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s",
		controllers[0], sub.EventBus,
		strings.Join(controllers, ","))
	return reader.Config{
		EventBusName:   sub.EventBus,
		EventBusVRN:    ebVrn,
		SubscriptionID: sub.ID,
	}
}
