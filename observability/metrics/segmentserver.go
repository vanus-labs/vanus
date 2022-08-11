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
	moduleOfSegmentServer = "segment_server"

	WriteTPSCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfSegmentServer,
		Name:      "write_event_count",
		Help:      "Total events for writing",
	}, []string{LabelVolume, LabelBlock})

	ReadTPSCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfSegmentServer,
		Name:      "read_event_count",
		Help:      "Total events for reading",
	}, []string{LabelVolume, LabelBlock})

	WriteThroughputCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfSegmentServer,
		Name:      "write_byte_count",
		Help:      "Total bytes for writing",
	}, []string{LabelVolume, LabelBlock})

	ReadThroughputCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: moduleOfSegmentServer,
		Name:      "read_byte_count",
		Help:      "Total bytes for reading or reading",
	}, []string{LabelVolume, LabelBlock})
)
