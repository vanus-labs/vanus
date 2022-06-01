module github.com/linkall-labs/vanus/client

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/linkall-labs/vanus/proto v0.1.0
	github.com/scylladb/go-set v1.0.2
	go.uber.org/atomic v1.9.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
)

replace (
	cloudevents.io/genproto v1.0.2 => ../proto/include/cloudevents/pkg
	github.com/linkall-labs/vanus/proto v0.1.0 => ../proto
	github.com/linkall-labs/vanus/raft v0.0.0 => ./../thirds/raft
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v0.0.0-20180701023420-4b7aa43c6742 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220218161850-94dd64e39d7c // indirect
)
