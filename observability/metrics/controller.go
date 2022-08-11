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
	moduleOfController = "controller"

	EventbusGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventbus_number",
		Help:      "The number of Eventbus.",
	})

	EventlogGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventlog_number",
		Help:      "The number of Eventlog.",
	}, []string{LabelEventbus})

	SegmentCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_number",
		Help:      "The number of Segment.",
	}, []string{LabelType})

	// BlockCounterVec move to store module?
	BlockCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "block_number",
		Help:      "The number of Block.",
	}, []string{LabelType})

	SubscriptionGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "subscription_number",
		Help:      "The number of subscription",
	}, []string{LabelEventbus})

	CtrlTriggerGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "trigger_number",
		Help:      "The number of trigger",
	}, []string{LabelTriggerWorker})
)
