module github.com/linkall-labs/vanus/proto

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/raft v0.5.1
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
)

replace (
	cloudevents.io/genproto => ./include/cloudevents/pkg
	github.com/linkall-labs/vanus/raft => ../raft
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	golang.org/x/net v0.0.0-20220909164309-bea034e7d591 // indirect
	golang.org/x/sys v0.0.0-20220728004956-3c1f35247d10 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220624142145-8cd45d7dbd1f // indirect
)
