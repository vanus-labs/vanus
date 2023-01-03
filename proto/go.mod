module github.com/linkall-labs/vanus/proto

go 1.18

require (
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/raft v0.5.1
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	golang.org/x/net v0.0.0-20221014081412-f15817d10f9b // indirect
	golang.org/x/sys v0.0.0-20220919091848-fb04ddd9f9c8 // indirect
	golang.org/x/text v0.4.0 // indirect
	google.golang.org/genproto v0.0.0-20221027153422-115e99e71e1c // indirect
)

replace (
	github.com/linkall-labs/vanus/observability => ../observability
	github.com/linkall-labs/vanus/raft => ../raft
)
