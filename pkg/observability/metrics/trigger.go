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
	moduleOfTriggerWorker = "trigger_worker"

	TriggerGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "trigger_number",
		Help:      "The number of trigger",
	}, []string{LabelTriggerWorker})

	TriggerPullEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "pull_event_number",
		Help:      "The event number of trigger pull",
	}, []string{LabelTrigger, LabelEventbus, LabelEventlog})

	TriggerFilterCostSecond = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "filter_cost_second",
		Help:      "The cost second of trigger",
	}, []string{LabelTrigger})

	TriggerTransformCostSecond = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "transformer_cost_second",
		Help:      "The cost second of transformer",
	}, []string{LabelTrigger})

	TriggerFilterMatchEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "filter_match_event_number",
		Help:      "The event number of trigger filter match",
	}, []string{LabelTrigger})

	TriggerFilterMatchRetryEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "filter_match_retry_event_number",
		Help:      "The event number of retry match",
	}, []string{LabelTrigger})

	TriggerRetryEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "retry_event_number",
		Help:      "The event number of retry",
	}, []string{LabelTrigger})

	TriggerRetryEventAppendSecond = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "retry_event_append_second",
		Help:      "The cost second of retry event append",
	}, []string{LabelTrigger})

	TriggerDeadLetterEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "dead_letter_event_number",
		Help:      "The event number of dead letter",
	}, []string{LabelTrigger})

	TriggerDeadLetterEventAppendSecond = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "dead_letter_event_append_second",
		Help:      "The cost second of dead letter event append",
	}, []string{LabelTrigger})

	TriggerPushEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "push_event_number",
		Help:      "The event number of trigger push",
	}, []string{LabelTrigger, LabelResult})

	TriggerPushEventTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "push_event_rt",
		Help:      "The rt of trigger push event",
	}, []string{LabelTrigger})
)
