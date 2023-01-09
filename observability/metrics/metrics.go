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

package metrics

import (
	"context"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/push"
	"time"
)

const (
	namespace = "vanus"
)

func PushMetrics(ctx context.Context, url, jobName string, coll []prometheus.Collector) {
	if len(coll) == 0 {
		return
	}
	p := push.New(url, jobName)
	for idx := range coll {
		p.Collector(coll[idx])
	}
	t := time.NewTimer(time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Info(ctx, "shutdown metrics pushing", nil)
		case <-t.C:
			if err := p.Push(); err != nil {
				log.Warning(ctx, "failed to push metrics", map[string]interface{}{
					log.KeyError: err,
				})
			}
		}
	}
}

func GetControllerMetrics() []prometheus.Collector {
	coll := []prometheus.Collector{
		EventbusGauge,
		EventlogGaugeVec,
		SegmentGaugeVec,
		SegmentCreationRuntimeCounterVec,
		SegmentDeletedCounterVec,
		SubscriptionGauge,
		SubscriptionTransformerGauge,
		CtrlTriggerGauge,
	}
	return append(coll, getGoRuntimeMetrics()...)
}

func GetTriggerMetrics() []prometheus.Collector {
	coll := []prometheus.Collector{
		TriggerGauge,
		TriggerPullEventCounter,
		TriggerFilterCostSecond,
		TriggerTransformCostSecond,
		TriggerFilterMatchEventCounter,
		TriggerFilterMatchRetryEventCounter,
		TriggerRetryEventCounter,
		TriggerRetryEventAppendSecond,
		TriggerDeadLetterEventCounter,
		TriggerDeadLetterEventAppendSecond,
		TriggerPushEventCounter,
		TriggerPushEventTime,
	}
	return append(coll, getGoRuntimeMetrics()...)
}

func GetTimerMetrics() []prometheus.Collector {
	coll := []prometheus.Collector{
		TimingWheelTickGauge,
		TimingWheelSizeGauge,
		TimingWheelLayersGauge,
		TimerPushEventTPSCounterVec,
		TimerDeliverEventTPSCounterVec,
		TimerScheduledEventDelayTime,
		TimerPushEventTime,
		TimerDeliverEventTime,
	}
	return append(coll, getGoRuntimeMetrics()...)
}

func GetSegmentServerMetrics() []prometheus.Collector {
	coll := []prometheus.Collector{
		WriteThroughputCounterVec,
		WriteTPSCounterVec,
		ReadTPSCounterVec,
		ReadThroughputCounterVec,
	}
	return append(coll, getGoRuntimeMetrics()...)
}

func getGoRuntimeMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		collectors.NewBuildInfoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics()),
	}
}
