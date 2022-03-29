module github.com/linkall-labs/vanus

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/cloudevents/sdk-go/sql/v2 v2.8.0
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/huandu/skiplist v1.2.0
	github.com/linkall-labs/eventbus-go v0.0.0
	github.com/linkall-labs/vsproto v0.0.0
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/goconvey v1.7.2
	go.etcd.io/etcd/client/v3 v3.5.2
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/client-go v0.23.4
)

replace (
	cloudevents.io/genproto v1.0.2 => ../vsproto/include/cloudevents/pkg
	github.com/linkall-labs/eventbus-go v0.0.0 => ../eventbus-go
	github.com/linkall-labs/vsproto v0.0.0 => ../vsproto
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20211221011931-643d94fcab96 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/go-logr/logr v1.2.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/scylladb/go-set v1.0.2 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220218161850-94dd64e39d7c // indirect
	k8s.io/apimachinery v0.23.4 // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
)
