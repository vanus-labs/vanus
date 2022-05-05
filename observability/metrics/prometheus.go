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
	"go.opentelemetry.io/otel/attribute"
)

// use prometheus as the backend of metrics
// TODO: implement

type promCounter struct {
}

func (pc *promCounter) IncrInt(n int64, attrs ...attribute.KeyValue) {}

func (pc *promCounter) IncrFloat(f float64, attrs ...attribute.KeyValue) {}

func (pc *promCounter) Async(f func(ctx context.Context, c ICounter)) {}

type promGauge struct{}

func (pg *promGauge) IncrInt(n int64, attrs ...attribute.KeyValue) {}

func (pg *promGauge) IncrFloat(f float64, attrs ...attribute.KeyValue) {}

func (pg *promGauge) Async(func(ctx context.Context, gauge IGauge)) {}

type promHistogram struct{}

func (ph *promHistogram) RecordInt(n int64, attrs ...attribute.KeyValue) {}

func (ph *promHistogram) RecordFloat(f float64, attrs ...attribute.KeyValue) {}

func (ph *promHistogram) Async(func(ctx context.Context, his IHistogram)) {}
