module github.com/vanus-labs/vanus/proto

go 1.19

require (
	github.com/cloudevents/sdk-go/v2 v2.14.0
	github.com/golang/mock v1.6.0
	github.com/vanus-labs/vanus/raft v0.8.0
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v0.0.0-20180701023420-4b7aa43c6742 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
)

replace (
	github.com/vanus-labs/vanus/observability => ../observability
	github.com/vanus-labs/vanus/raft => ../raft
)

replace github.com/vanus-labs/vanus => ../FORBIDDEN_DEPENDENCY
