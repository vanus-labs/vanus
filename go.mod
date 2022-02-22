module github.com/linkall-labs/vanus

go 1.17

require (
	github.com/cloudevents/sdk-go/sql/v2 v2.8.0
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/google/uuid v1.3.0
	github.com/linkall-labs/eventbus-go v0.0.0
	github.com/linkall-labs/vsproto v0.0.0
	github.com/sirupsen/logrus v1.8.1
	google.golang.org/grpc v1.44.0
	go.etcd.io/etcd/client/v3 v3.5.2
)

replace (
	github.com/linkall-labs/eventbus-go v0.0.0 => ../eventbus-go
	github.com/linkall-labs/vsproto v0.0.0 => ../vsproto
	cloudevents.io/genproto v1.0.2 => ../vsproto/include/cloudevents/pkg
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20211221011931-643d94fcab96 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v0.0.0-20180701023420-4b7aa43c6742 // indirect
	github.com/scylladb/go-set v1.0.2 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0 // indirect
	golang.org/x/net v0.0.0-20200822124328-c89045814202 // indirect
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd // indirect
	golang.org/x/text v0.3.0 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	cloudevents.io/genproto v1.0.2 // indirect
)
