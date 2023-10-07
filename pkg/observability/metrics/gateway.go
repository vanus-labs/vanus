// Copyright 2023 Linkall Inc.
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
	moduleOfGateway = "gateway"

	GatewayEventReceivedCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfGateway,
		Name:      "event_received_total",
	}, []string{LabelEventbus, LabelProtocol, LabelBatchSize, LabelResponseCode})

	GatewayEventWriteLatencyHistogramVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: moduleOfGateway,
		Name:      "send_event_request_latency_histogram",
		Buckets: []float64{0.4, 0.7, 1, 2, 3, 5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90,
			100, 200, 300, 400, 500, 1000}, // ms
		NativeHistogramBucketFactor:     0,
		NativeHistogramZeroThreshold:    0,
		NativeHistogramMaxBucketNumber:  0,
		NativeHistogramMinResetDuration: 0,
		NativeHistogramMaxZeroThreshold: 0,
	}, []string{LabelEventbus, LabelProtocol, LabelBatchSize})

	GatewayEventWriteLatencySummaryVec = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace,
		Subsystem: moduleOfGateway,
		Name:      "send_event_request_latency_summary",
		Objectives: map[float64]float64{
			0.25:   0.1,
			0.50:   0.1,
			0.75:   0.1,
			0.80:   0.05,
			0.85:   0.05,
			0.9:    0.05,
			0.95:   0.01,
			0.96:   0.01,
			0.97:   0.01,
			0.98:   0.01,
			0.99:   0.001,
			0.999:  0.0001,
			0.9999: 0.00001},
	}, []string{LabelEventbus, LabelProtocol, LabelBatchSize})
)
