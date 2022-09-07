module github.com/linkall-labs/vanus/client

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/cloudevents/sdk-go/v2 v2.11.0
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/proto v0.3.0-alpha
	github.com/scylladb/go-set v1.0.2
	go.uber.org/atomic v1.9.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
)

replace (
	cloudevents.io/genproto => ../proto/include/cloudevents/pkg
	github.com/linkall-labs/vanus/proto => ../proto
	github.com/linkall-labs/vanus/raft => ../raft
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220310185008-1973136f34c6 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
