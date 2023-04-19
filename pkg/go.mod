module github.com/vanus-labs/vanus/pkg

go 1.19

require (
	github.com/golang/mock v1.6.0
	github.com/ohler55/ojg v1.18.4
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.8.0
	github.com/vanus-labs/vanus/observability v0.8.0
	github.com/vanus-labs/vanus/proto v0.8.0
	golang.org/x/oauth2 v0.7.0
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/rs/zerolog v1.29.1 // indirect
	github.com/smartystreets/assertions v1.13.1 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sys v0.7.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
)

replace (
	github.com/vanus-labs/vanus/observability => ../observability
	github.com/vanus-labs/vanus/proto => ../proto
	github.com/vanus-labs/vanus/raft => ../raft
)

replace github.com/vanus-labs/vanus => ../FORBIDDEN_DEPENDENCY
