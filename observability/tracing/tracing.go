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
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	oteltracer "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	otelCollector = "OTEL_COLLECTOR_ENDPOINT"
	serverName    = "SERVER_NAME"
)

var (
	tp *tracerProvider
)

func Init(name ...string) {
	srvName := os.Getenv(serverName)
	if len(name) > 0 {
		srvName = name[0]
	}
	p := &tracerProvider{
		serverName: srvName,
	}
	endpoint := os.Getenv(otelCollector)
	if endpoint != "" {
		provider, err := newTracerProvider(p.serverName, endpoint)
		if err != nil {
			panic("init tracer error: " + err.Error())
		}
		p.p = provider
	} else {
		p.p = oteltracer.NewNoopTracerProvider()
	}
	tp = p
}

func Start(ctx context.Context, pkgName, methodName string) (context.Context, oteltracer.Span) {
	if tp == nil {
		return ctx, emptySpan("test")
	}
	return tp.p.Tracer(pkgName).Start(ctx, strings.Join([]string{pkgName, methodName}, "/"),
		oteltracer.WithSpanKind(oteltracer.SpanKindServer))
}

type tracerProvider struct {
	p          oteltracer.TracerProvider
	serverName string
}

type Tracer struct {
	tracer     oteltracer.Tracer
	kind       oteltracer.SpanKind
	moduleName string
}

func (t *Tracer) Start(ctx context.Context, methodName string) (context.Context, oteltracer.Span) {
	if t == nil {
		return ctx, emptySpan("test")
	}
	return t.tracer.Start(ctx, strings.Join([]string{t.moduleName, methodName}, "/"),
		oteltracer.WithSpanKind(t.kind))
}

func NewTracer(moduleName string, kind oteltracer.SpanKind) *Tracer {
	if tp == nil {
		return &Tracer{
			tracer:     oteltracer.NewNoopTracerProvider().Tracer(moduleName),
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
			semconv.ServiceVersionKey.String("v0.3.0"),
			attribute.String("environment", "local"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
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

func newJaegerProvider(service string, jaegerURL string) (*trace.TracerProvider, error) {
	// Create the Jaeger exporter
	endpoint := jaeger.WithCollectorEndpoint()
	if jaegerURL != "" {
		endpoint = jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerURL))
	}
	exp, err := jaeger.New(endpoint)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		// Always be sure to batch in production.
		trace.WithBatcher(exp, trace.WithBatchTimeout(time.Second)),
		// Record information about this application in a Resource.
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
		)),
	)
	return tp, nil
}
