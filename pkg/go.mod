module github.com/vanus-labs/vanus/pkg

go 1.18

require (
	github.com/golang/mock v1.6.0
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.7.2
	github.com/vanus-labs/vanus/observability v0.5.7
	github.com/vanus-labs/vanus/proto v0.5.7
	golang.org/x/oauth2 v0.6.0
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181017120253-0766667cb4d1 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/rs/zerolog v1.29.0 // indirect
	github.com/smartystreets/assertions v1.2.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20221202195650-67e5cbc046fd // indirect
)

replace (
	github.com/vanus-labs/vanus/observability => ../observability
	github.com/vanus-labs/vanus/proto => ../proto
	github.com/vanus-labs/vanus/raft => ../raft
)
