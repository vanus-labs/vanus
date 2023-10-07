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

	ControllerLeaderGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "leader_flag",
		Help:      "if is this controller leader",
	}, []string{LabelIsLeader})

	EventbusGauge = prometheus.NewGauge(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventbus_number_total",
	})

	EventbusUpdatedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventbus_number_updated_total",
	})

	EventbusDeletedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventbus_number_deleted_total",
	})

	// EventlogGaugeVec TODO wenfeng clean values?
	EventlogGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "eventlog_number_total_per_eventbus",
	}, []string{LabelEventbus})

	SegmentGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_number_total_per_eventlog",
	}, []string{LabelEventbus, LabelEventlog, LabelSegmentState})

	SegmentSizeGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_size_total_per_eventlog",
	}, []string{LabelEventbus, LabelEventlog, LabelSegmentState})

	SegmentCapacityGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_capacity_total_per_eventlog",
	}, []string{LabelEventbus, LabelEventlog, LabelSegmentState})

	SegmentEventNumberGaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_event_number_total_per_eventlog",
	}, []string{LabelEventbus, LabelEventlog, LabelSegmentState})

	SegmentCreatedByCacheMissing = prometheus.NewGauge(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_created_by_cache_missing_total",
	})

	SegmentCreatedByScaleTask = prometheus.NewGauge(prometheus.GaugeOpts{ // visualized
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_created_by_scale_task_total",
	})

	SegmentDeletedCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfController,
		Name:      "segment_deleted_number_runtime",
	}, []string{LabelDeletedReason})

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
