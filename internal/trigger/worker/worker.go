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
	"fmt"
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
}

type subscriptionWorker struct {
	subscription *primitive.Subscription
	trigger      *trigger.Trigger
	events       chan info.EventOffset
	reader       reader.Reader
	stopTime     time.Time
	startTime    *time.Time
}

func NewSubscriptionWorker(subscription *primitive.Subscription,
	subscriptionOffset *offset.SubscriptionOffset,
	controllers []string) SubscriptionWorker {
	sw := &subscriptionWorker{
		events:       make(chan info.EventOffset, eventBufferSize),
		subscription: subscription,
	}
	offsetMap := make(map[vanus.ID]uint64)
	for _, o := range subscription.Offsets {
		offsetMap[o.EventLogID] = o.Offset
	}
	sw.reader = reader.NewReader(getReaderConfig(subscription, controllers), offsetMap, sw.events)
	triggerConf := &trigger.Config{}
	sw.trigger = trigger.NewTrigger(triggerConf, subscription, subscriptionOffset)
	return sw
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
	w.startTime = &now
	return nil
}

func (w *subscriptionWorker) Stop(ctx context.Context) {
	if w.startTime == nil {
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
