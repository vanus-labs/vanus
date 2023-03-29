module github.com/vanus-labs/vanus/proto

go 1.18

require (
	github.com/cloudevents/sdk-go/v2 v2.13.0
	github.com/golang/mock v1.6.0
	github.com/vanus-labs/vanus/raft v0.5.7
	google.golang.org/grpc v1.51.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/stretchr/testify v1.8.1 // indirect
	golang.org/x/net v0.4.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20221202195650-67e5cbc046fd // indirect
)

replace (
	github.com/vanus-labs/vanus/observability => ../observability
	github.com/vanus-labs/vanus/raft => ../raft
)
