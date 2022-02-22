module github.com/linkall-labs/vanus

go 1.17

require (
	github.com/cloudevents/sdk-go/sql/v2 v2.8.0
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/google/uuid v1.3.0
	github.com/linkall-labs/eventbus-go v0.0.0
	github.com/linkall-labs/vsproto v0.0.0
	github.com/sirupsen/logrus v1.8.1
	go.etcd.io/etcd/client/v3 v3.5.2
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	github.com/golang/protobuf v1.5.2
)

replace (
	cloudevents.io/genproto v1.0.2 => ../vsproto/include/cloudevents/pkg
	github.com/linkall-labs/eventbus-go v0.0.0 => ../eventbus-go
	github.com/linkall-labs/vsproto v0.0.0 => ../vsproto
)

require (
	cloudevents.io/genproto v1.0.2 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20211221011931-643d94fcab96 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/scylladb/go-set v1.0.2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20210603081109-ebe580a85c40 // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
)
