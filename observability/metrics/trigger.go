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
		Help:      "The number of Trigger.",
	}, []string{LabelEventbus, LabelTriggerWorker})

	TriggerPullEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "pull_event_number",
		Help:      "The event number of Trigger Pull",
	}, []string{LabelTrigger, LabelEventlog})

	TriggerFilterMatchCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "filter_match_number",
		Help:      "The event number of Trigger Filter Match",
	}, []string{LabelTrigger})

	TriggerPushEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "push_event_number",
		Help:      "The event number of Trigger Push",
	}, []string{LabelTrigger, LabelResult})

	TriggerPushEventRtCounter = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "push_event_rt",
		Help:      "The rt of Trigger Push Event",
	}, []string{LabelTrigger})
)
