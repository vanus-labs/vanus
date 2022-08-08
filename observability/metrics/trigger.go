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

	TriggerPullCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "trigger pull tps",
		Help:      "The tps of Trigger Pull",
	}, []string{LabelTrigger, LabelEventlog})

	TriggerPushCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfTriggerWorker,
		Name:      "trigger push tps",
		Help:      "The tps of Trigger Push",
	}, []string{LabelTrigger})
)
