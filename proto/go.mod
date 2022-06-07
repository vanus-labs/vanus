module github.com/linkall-labs/vanus/proto

go 1.17

require (
	cloudevents.io/genproto v1.0.2
	github.com/golang/mock v1.6.0
	github.com/linkall-labs/vanus/raft v0.0.0
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
)

replace (
	cloudevents.io/genproto => ./include/cloudevents/pkg
	github.com/linkall-labs/vanus/raft => ../raft
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	golang.org/x/net v0.0.0-20201021035429-f5854403a974 // indirect
	golang.org/x/sys v0.0.0-20210603125802-9665404d3644 // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
)
