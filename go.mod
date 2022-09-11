module github.com/linkall-labs/vanus

go 1.18

require (
	cloudevents.io/genproto v1.0.2
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/aws/aws-sdk-go-v2 v1.16.11
	github.com/aws/aws-sdk-go-v2/credentials v1.12.13
	github.com/aws/aws-sdk-go-v2/service/lambda v1.23.8
	github.com/cloudevents/sdk-go/sql/v2 v2.10.1
	github.com/cloudevents/sdk-go/v2 v2.11.0
	github.com/fatih/color v1.13.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-resty/resty/v2 v2.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/cel-go v0.11.2
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/huandu/skiplist v1.2.0
	github.com/iceber/iouring-go v0.0.0-20220609112130-b1dc8dd9fbfd
	github.com/jedib0t/go-pretty/v6 v6.3.1
	github.com/labstack/echo/v4 v4.7.2
	github.com/linkall-labs/embed-etcd v0.1.0
	github.com/linkall-labs/vanus/client v0.3.0-alpha
	github.com/linkall-labs/vanus/observability v0.4.0
	github.com/linkall-labs/vanus/pkg v0.3.0-alpha
	github.com/linkall-labs/vanus/proto v0.4.0
	github.com/linkall-labs/vanus/raft v0.3.0-alpha
	github.com/mwitkow/grpc-proxy v0.0.0
	github.com/ncw/directio v1.0.5
	github.com/prashantv/gostub v1.1.0
	github.com/prometheus/client_golang v1.13.0
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.4.0
	github.com/tidwall/gjson v1.14.1
	go.etcd.io/etcd/client/v3 v3.6.0-alpha.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.34.0
	go.opentelemetry.io/otel v1.9.0
	go.opentelemetry.io/otel/trace v1.9.0
	go.uber.org/ratelimit v0.2.0
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8
	google.golang.org/genproto v0.0.0-20220310185008-1973136f34c6
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/apimachinery v0.25.0
	k8s.io/client-go v0.25.0
	k8s.io/klog/v2 v2.80.0
)

require (
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129 // indirect
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20220209173558-ad29539cd2e9 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.12 // indirect
	github.com/aws/smithy-go v1.12.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/labstack/gommon v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/scylladb/go-set v1.0.2 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/stretchr/testify v1.8.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/api/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/client/v2 v2.306.0-alpha.0 // indirect
	go.etcd.io/etcd/pkg/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/raft/v3 v3.6.0-alpha.0 // indirect
	go.etcd.io/etcd/server/v3 v3.6.0-alpha.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.9.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.9.0 // indirect
	go.opentelemetry.io/otel/sdk v1.9.0 // indirect
	go.opentelemetry.io/proto/otlp v0.18.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/crypto v0.0.0-20220315160706-3147a52a75dd // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/utils v0.0.0-20220728103510-ee6ede2d64ed // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	cloudevents.io/genproto => ./proto/include/cloudevents/pkg
	github.com/linkall-labs/vanus/client => ./client
	github.com/linkall-labs/vanus/observability => ./observability
	github.com/linkall-labs/vanus/pkg => ./pkg
	github.com/linkall-labs/vanus/proto => ./proto
	github.com/linkall-labs/vanus/raft => ./raft
	github.com/mwitkow/grpc-proxy => github.com/linkall-labs/grpc-proxy v0.0.0-20220624142509-a3b0cb2bb86c
)
