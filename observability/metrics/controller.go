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
		Name:      "eventbus_number_total",
		Help:      "The number of Eventbus.",
	})

	EventlogGaugeVec = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventlog_number_total",
		Help:      "The Eventlog number.",
	})

	SegmentGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_number_per_eventbus_total",
		Help:      "The Segment's number of each eventbus .",
	}, []string{LabelEventbus})

	SegmentCreationRuntimeCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_creation_runtime",
		Help:      "The runtime info about segment creation.",
	}, []string{LabelType})

	SegmentDeletedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "deleted_segment_number_runtime",
		Help:      "The number of deleted Segment.",
	}, []string{LabelType})

	SubscriptionGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "subscription_number",
		Help:      "The number of subscription",
	}, []string{LabelEventbus})

	SubscriptionTransformerGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "subscription_transformer_number",
		Help:      "The number of subscription transformer",
	}, []string{LabelEventbus})

	CtrlTriggerGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "trigger_number",
		Help:      "The number of trigger",
	}, []string{LabelTriggerWorker})
)
