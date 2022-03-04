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
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial("192.168.1.111:11811", opts...)
	if err != nil {
		log.Fatal(err.Error(), nil)
	}
	cli := segment.NewSegmentServerClient(conn)
	TestAppend(cli)
	conn.Close()
}

func TestAppend(cli segment.SegmentServerClient) {
	id := "1646365657626669000"
	cnt := 0
	for idx := 0; idx < 50000; idx++ {
		cnt++
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
						Data: &v1.CloudEvent_TextData{TextData: "asdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsda" +
							"sdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsda" +
							"sdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdas" +
							"dasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasd" +
							"asdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasda" +
							"sdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdas" +
							"dsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasd" +
							"sdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasds" +
							"dasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsd" +
							"asdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsda" +
							"sdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdas" +
							"dasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasd" +
							"asdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasda" +
							"sdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdas" +
							"dsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasd" +
							"sdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasdsdasdasds"},
					},
				},
			},
		})
		if err != nil {
			log.Fatal(err.Error(), map[string]interface{}{
				"sent": cnt,
			})
		}
	}
}

func TestRead() {

}
