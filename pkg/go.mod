module github.com/linkall-labs/vanus/pkg

go 1.18

require (
	github.com/linkall-labs/vanus/observability v0.5.1
	github.com/linkall-labs/vanus/proto v0.5.1
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.7.2
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
)

replace (
	cloudevents.io/genproto => ../proto/include/cloudevents/pkg
	github.com/linkall-labs/vanus/observability => ../observability
	github.com/linkall-labs/vanus/proto => ../proto
	github.com/linkall-labs/vanus/raft => ../raft
)

require (
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	golang.org/x/net v0.0.0-20220909164309-bea034e7d591 // indirect
	golang.org/x/sys v0.0.0-20220728004956-3c1f35247d10 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220624142145-8cd45d7dbd1f // indirect
)
