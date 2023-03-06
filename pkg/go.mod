module github.com/linkall-labs/vanus/pkg

go 1.18

require (
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/proto v0.5.7
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.7.2
	github.com/vanus-labs/vanus/observability v0.5.7
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	golang.org/x/net v0.4.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20221027153422-115e99e71e1c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/linkall-labs/vanus/proto => ../proto
	github.com/vanus-labs/vanus/observability => ../observability
	github.com/vanus-labs/vanus/raft => ../raft
)
