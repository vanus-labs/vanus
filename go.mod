module github.com/linkall-labs/vanus

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/cloudevents/sdk-go/sql/v2 v2.8.0
	github.com/cloudevents/sdk-go/v2 v2.8.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/huandu/skiplist v1.2.0
	github.com/linkall-labs/embed-etcd v0.0.0
	github.com/linkall-labs/eventbus-go v0.0.0
	github.com/linkall-labs/raft v0.0.0
	github.com/linkall-labs/vsproto v0.0.0
	github.com/pkg/errors v0.9.1
	github.com/prashantv/gostub v1.1.0
	github.com/prometheus/client_golang v1.11.1
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.4.0
	go.etcd.io/etcd/client/v3 v3.5.3
	go.opentelemetry.io/otel v0.20.0
	go.uber.org/atomic v1.9.0
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/client-go v0.23.4
)

replace (
	cloudevents.io/genproto v1.0.2 => ../vsproto/include/cloudevents/pkg
	github.com/linkall-labs/embed-etcd v0.0.0 => ../embed-etcd
	github.com/linkall-labs/eventbus-go v0.0.0 => ../eventbus-go
	github.com/linkall-labs/raft v0.0.0 => ../raft
	github.com/linkall-labs/vsproto v0.0.0 => ../vsproto
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20211221011931-643d94fcab96 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/scylladb/go-set v1.0.2 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/api/v3 v3.5.3 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.3 // indirect
	go.etcd.io/etcd/client/v2 v2.305.2 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.2 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.2 // indirect
	go.etcd.io/etcd/server/v3 v3.5.2 // indirect
	go.opentelemetry.io/contrib v0.20.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/export/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v0.20.0 // indirect
	go.opentelemetry.io/proto/otlp v0.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sys v0.0.0-20220209214540-3681064d5158 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220218161850-94dd64e39d7c // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apimachinery v0.23.4 // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)
