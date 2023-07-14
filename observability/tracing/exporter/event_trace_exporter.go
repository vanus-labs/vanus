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

package exporter

import (
	"context"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	vanussdk "github.com/vanus-labs/sdk/golang"
	"github.com/vanus-labs/vanus/observability/log"

	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

type Option func(*Options)

type Options struct {
	Endpoints string
	Eventbus  string
}

func defaultOptions() *Options {
	return &Options{
		Endpoints: "127.0.0.1:8080",
		Eventbus:  "event-tracing",
	}
}

func WithEndpoint(endpoint string) Option {
	return func(options *Options) {
		options.Endpoints = endpoint
	}
}

func WithEventbus(eventbus string) Option {
	return func(options *Options) {
		options.Eventbus = eventbus
	}
}

// Exporter exports trace data in the OTLP wire format.
type Exporter struct {
	endpoints string
	client    vanussdk.Client
	publisher vanussdk.Publisher
}

var _ tracesdk.SpanExporter = (*Exporter)(nil)

func New(ctx context.Context, opts ...Option) (*Exporter, error) {
	defaultOpts := defaultOptions()
	for _, apply := range opts {
		apply(defaultOpts)
	}

	clientOpts := &vanussdk.ClientOptions{
		Endpoint: defaultOpts.Endpoints,
		Token:    "admin",
	}

	c, err := vanussdk.Connect(clientOpts)
	if err != nil {
		panic("failed to connect to Vanus cluster, error: " + err.Error())
	}

	ebOpt := vanussdk.WithEventbus("default", defaultOpts.Eventbus)
	exporter := &Exporter{
		endpoints: defaultOpts.Endpoints,
		client:    c,
		publisher: c.Publisher(ebOpt),
	}
	_, err = c.Controller().Eventbus().Get(ctx, ebOpt)
	if err != nil {
		panic("failed to get tracing eventbus, error: " + err.Error())
	}
	return exporter, nil
}

// ExportSpans exports a batch of spans.
func (e *Exporter) ExportSpans(ctx context.Context, ss []tracesdk.ReadOnlySpan) error {
	es := make([]*v2.Event, 0)
	for _, span := range ss {
		if span.Name() != "EventTracing" {
			continue
		}
		event := newEvent(span)
		es = append(es, &event)
	}

	if len(es) == 0 {
		return nil
	}

	err := e.publisher.Publish(ctx, es...)
	if err != nil {
		log.Error(ctx).Err(err).Msg("failed to publish events to tracing eventbus")
		return nil
	}
	return nil
}

// Shutdown flushes all exports and closes all connections to the receiving endpoint.
func (e *Exporter) Shutdown(ctx context.Context) error {
	return nil
}

func newEvent(span tracesdk.ReadOnlySpan) v2.Event {
	event := v2.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource(span.Name())
	event.SetType(span.SpanKind().String())
	data := make(map[string]interface{})
	data["name"] = span.Name()
	data["trace_id"] = span.SpanContext().TraceID().String()
	data["span_id"] = span.SpanContext().SpanID().String()
	data["span_kind"] = span.SpanKind().String()
	data["start_time"] = span.StartTime()
	data["end_time"] = span.EndTime()
	data["status"] = span.Status()
	for _, attr := range span.Attributes() {
		if attr.Value.Type() == attribute.INT64 {
			data[string(attr.Key)] = attr.Value.AsInt64()
		} else if attr.Value.Type() == attribute.STRING {
			data[string(attr.Key)] = attr.Value.AsString()
		}
	}
	data["events"] = span.Events()
	_ = event.SetData(v2.ApplicationJSON, data)
	return event
}
