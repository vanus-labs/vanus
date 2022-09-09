module github.com/linkall-labs/vanus/client

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/cloudevents/sdk-go/v2 v2.11.0
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/observability v0.4.0
	github.com/linkall-labs/vanus/pkg v0.3.0-alpha
	github.com/linkall-labs/vanus/proto v0.4.0
	github.com/scylladb/go-set v1.0.2
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.34.0
	go.opentelemetry.io/otel/trace v1.9.0
	go.uber.org/atomic v1.9.0
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v0.0.0-20180701023420-4b7aa43c6742 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	go.opentelemetry.io/otel v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.9.0 // indirect
	go.opentelemetry.io/otel/sdk v1.9.0 // indirect
	go.opentelemetry.io/proto/otlp v0.18.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220310185008-1973136f34c6 // indirect
)

replace (
	cloudevents.io/genproto => ../proto/include/cloudevents/pkg
	github.com/linkall-labs/vanus/observability => ../observability
	github.com/linkall-labs/vanus/pkg => ../pkg
	github.com/linkall-labs/vanus/proto => ../proto
	github.com/linkall-labs/vanus/raft => ../raft
)
