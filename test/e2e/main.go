// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	v1 "cloudevents.io/genproto/v1"
	"context"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// TODO
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial("localhost:11811", opts...)
	if err != nil {
		log.Fatal(err.Error(), nil)
	}
	cli := segment.NewSegmentServerClient(conn)
	TestAppend(cli)
	conn.Close()
}

func TestAppend(cli segment.SegmentServerClient) {
	id := "1231sss23123"
	_, err := cli.Start(context.Background(), &segment.StartSegmentServerRequest{
		Config:          nil,
		SegmentServerId: "human-1",
	})
	if err != nil {
		//panic(err)
	}
	_, err = cli.CreateSegmentBlock(context.Background(), &segment.CreateSegmentBlockRequest{
		Id:   id,
		Size: 64 * 1024 * 1024,
	})
	if err != nil {
		//log.Fatal(err.Error(), nil)
	}
	for idx := 0; idx < 100; idx++ {
		_, err := cli.AppendToSegment(context.Background(), &segment.AppendToSegmentRequest{
			SegmentId: id,
			Events: &v1.CloudEventBatch{
				Events: []*v1.CloudEvent{
					{
						Id:          "asdsadsadasdsadsadasdsadsad",
						Source:      "asdsadsadasdsadsadasdsadsad",
						SpecVersion: "asdsadsadasdsadsadasdsadsad",
						Type:        "asdsadsadasdsadsadasdsadsad",
						Attributes:  nil,
						Data:        &v1.CloudEvent_TextData{TextData: "asdsdasdasdsdasdasdsdasdasdsdasd"},
					},
				},
			},
		})
		if err != nil {
			log.Fatal(err.Error(), nil)
		}
	}
}

func TestRead() {

}
