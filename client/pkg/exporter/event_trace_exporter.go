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
	"os"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

func GetExporter(endpoints []string, eventbus string) tracesdk.SpanExporter {
	spanExporter, err := New(context.Background(), WithEndpoints(endpoints), WithEventbus(eventbus))
	if err != nil {
		log.Error(context.Background(), "new span exporter failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	return spanExporter
}

type Option func(*Options)

type Options struct {
	Endpoints []string
	Eventbus  string
}

func defaultOptions() *Options {
	return &Options{
		Endpoints: []string{},
		Eventbus:  "event-tracing",
	}
}

func WithEndpoints(endpoints []string) Option {
	return func(options *Options) {
		options.Endpoints = endpoints
	}
}

func WithEventbus(eventbus string) Option {
	return func(options *Options) {
		options.Eventbus = eventbus
	}
}

// Exporter exports trace data in the OTLP wire format.
type Exporter struct {
	endpoints []string
	writer    api.BusWriter
}

var _ tracesdk.SpanExporter = (*Exporter)(nil)

func New(ctx context.Context, opts ...Option) (*Exporter, error) {
	defaultOpts := defaultOptions()
	for _, apply := range opts {
		apply(defaultOpts)
	}

	ctrl := cluster.NewClusterController(defaultOpts.Endpoints, insecure.NewCredentials())
	if err := ctrl.WaitForControllerReady(true); err != nil {
		log.Error(ctx, "wait for controller ready timeout", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}
	eventbus, err := ctrl.EventbusService().GetEventbusByName(ctx, "default", defaultOpts.Eventbus)
	if err != nil {
		log.Error(ctx, "failed to get eventbus", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   defaultOpts.Eventbus,
		})
		return nil, err
	}

	c := client.Connect(defaultOpts.Endpoints)
	bus := c.Eventbus(ctx, api.WithName(defaultOpts.Eventbus), api.WithID(eventbus.Id))
	exporter := &Exporter{
		endpoints: defaultOpts.Endpoints,
		writer:    bus.Writer(),
	}
	return exporter, nil
}

// ExportSpans exports a batch of spans.
func (e *Exporter) ExportSpans(ctx context.Context, ss []tracesdk.ReadOnlySpan) error {
	ces := make([]*cloudevents.CloudEvent, 0)
	for _, span := range ss {
		event := newEvent(span)
		if event.Type() != "event-tracing" {
			continue
		}
		eventpb, err := codec.ToProto(&event)
		if err != nil {
			log.Error(ctx, "failed to proto event", map[string]interface{}{
				log.KeyError: err,
				"event":      event,
			})
			return nil
		}
		ces = append(ces, eventpb)
	}

	if len(ces) == 0 {
		return nil
	}

	ceBatch := &cloudevents.CloudEventBatch{
		Events: ces,
	}
	_, err := e.writer.Append(ctx, ceBatch)
	if err != nil {
		log.Error(ctx, "failed to append events to tracing eventbus", map[string]interface{}{
			log.KeyError: err,
		})
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
			if string(attr.Key) == "type" && attr.Value.AsString() == "event-tracing" {
				event.SetType("event-tracing")
			}
		}
	}
	data["events"] = span.Events()
	_ = event.SetData(v2.ApplicationJSON, data)
	return event
}
