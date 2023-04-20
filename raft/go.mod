module github.com/vanus-labs/vanus/raft

go 1.19

require (
	github.com/cockroachdb/datadriven v1.0.2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.3
	go.etcd.io/etcd/client/pkg/v3 v3.5.8
	go.opentelemetry.io/otel/trace v1.14.0
)

require (
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel v1.14.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/vanus-labs/vanus/observability => ../observability

// Bad imports are sometimes causing attempts to pull that code.
// This makes the error more explicit.
replace (
	github.com/vanus-labs/vanus => ../FORBIDDEN_DEPENDENCY
	go.etcd.io/etcd => ./FORBIDDEN_DEPENDENCY
	go.etcd.io/etcd/v3 => ./FORBIDDEN_DEPENDENCY
)
