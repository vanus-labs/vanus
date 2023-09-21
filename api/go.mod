module github.com/vanus-labs/vanus/api

go 1.19

require (
	github.com/cloudevents/sdk-go/v2 v2.14.0
	github.com/golang/mock v1.6.0
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.8.1
	github.com/vanus-labs/vanus/pkg/observability v0.0.0-00010101000000-000000000000
	github.com/vanus-labs/vanus/pkg/raft v0.9.0
	golang.org/x/oauth2 v0.7.0
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rs/zerolog v1.29.1 // indirect
	github.com/smarty/assertions v1.15.0 // indirect
	github.com/stretchr/testify v1.8.2 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
)

replace (
	github.com/vanus-labs/vanus/pkg/observability => ../pkg/observability
	github.com/vanus-labs/vanus/pkg/raft => ../pkg/raft
)

replace github.com/vanus-labs/vanus => ../FORBIDDEN_DEPENDENCY
