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

package tracing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vanus-labs/vanus/observability/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	vanusVersion     = "v0.5.0"
	environmentKey   = "environment"
	environmentValue = "local"
)

type Config struct {
	ServerName    string `yaml:"-"`
	Enable        bool   `yaml:"enable"`
	OtelCollector string `yaml:"otel_collector"`
}

var tp *tracerProvider

func Init(cfg Config) {
	if cfg.ServerName == "" {
		log.Info().Msg("tracing name is empty, ignored")
		return
	}
	p := &tracerProvider{
		serverName: cfg.ServerName,
	}
	if cfg.Enable {
		if cfg.OtelCollector != "" {
			provider, err := newTracerProvider(p.serverName, cfg.OtelCollector)
			if err != nil {
				panic("init tracer error: " + err.Error())
			}
			p.p = provider
			log.Info().Str("otel_collector", cfg.OtelCollector).Msg("tracing module started, OpenTelemetry is enable")
		} else {
			// if otel_collector is empty, switch to noop tracer
			log.Warn().Str("otel_collector", cfg.OtelCollector).Msg("tracing module is enabled," +
				" but otel_collector is empty, switch to noop tracer")
			p.p = oteltrace.NewNoopTracerProvider()
		}
	} else {
		p.p = oteltrace.NewNoopTracerProvider()
	}
	tp = p
}

// Test input two num, return sum
func Test() {

}

func Start(ctx context.Context, pkgName, methodName string) (context.Context, oteltrace.Span) {
	if tp == nil {
		return ctx, emptySpan("test")
	}
	return tp.p.Tracer(pkgName).Start(ctx, strings.Join([]string{pkgName, methodName}, "/"),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer))
}

type tracerProvider struct {
	p          oteltrace.TracerProvider
	serverName string
}

type Tracer struct {
	tracer     oteltrace.Tracer
	kind       oteltrace.SpanKind
	moduleName string
}

func (t *Tracer) Start(ctx context.Context, methodName string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	if t == nil {
		return ctx, emptySpan("test")
	}
	return t.tracer.Start(ctx, strings.Join([]string{t.moduleName, methodName}, "/"),
		append(opts, oteltrace.WithSpanKind(t.kind))...)
}

func NewTracer(moduleName string, kind oteltrace.SpanKind) *Tracer {
	if tp == nil {
		return &Tracer{
			tracer:     oteltrace.NewNoopTracerProvider().Tracer(moduleName),
			kind:       kind,
			moduleName: moduleName,
		}
	}

	return &Tracer{
		tracer:     tp.p.Tracer(moduleName),
		kind:       kind,
		moduleName: moduleName,
	}
}

func newTracerProvider(serviceName string, collectorEndpoint string) (*trace.TracerProvider, error) {
	ctx := context.Background()
	res, err := resource.New(ctx, resource.WithContainer())
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}
	res, err = resource.Merge(resource.Default(), res)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}
	res, err = resource.Merge(
		res,
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(vanusVersion),
			attribute.String(environmentKey, environmentValue),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, collectorEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector[ %s ]: %w",
			collectorEndpoint, err)
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := trace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(res),
		trace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider, nil
}
