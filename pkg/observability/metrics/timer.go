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

import "github.com/prometheus/client_golang/prometheus"

var (
	moduleOfTimer = "timer"

	TimingWheelTickGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfTimer,
		Name:      "timingwheel_tick",
		Help:      "The tick of timingwheel.",
	})

	TimingWheelSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfTimer,
		Name:      "timingwheel_size",
		Help:      "The size of timingwheel.",
	})

	TimingWheelLayersGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfTimer,
		Name:      "timingwheel_layers",
		Help:      "The layers of timingwheel.",
	})

	TimerPushEventTPSCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTimer,
		Name:      "push_scheduled_event_count",
		Help:      "Total scheduled events for push",
	}, []string{LabelTimer})

	TimerDeliverEventTPSCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTimer,
		Name:      "deliver_scheduled_event_count",
		Help:      "Total scheduled events for deliver",
	}, []string{LabelTimer})

	TimerScheduledEventDelayTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "scheduled_event_delay_time",
		Help:      "The time of scheduled event delay deliver",
	}, []string{LabelScheduledEventDelayTime})

	TimerPushEventTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "push_scheduled_event_time",
		Help:      "The time of timer push scheduled event",
	}, []string{LabelTimerPushScheduledEventTime})

	TimerDeliverEventTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "deliver_scheduled_event_time",
		Help:      "The time of timer deliver scheduled event",
	}, []string{LabelTimerDeliverScheduledEventTime})
)
