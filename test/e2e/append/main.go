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
	"fmt"
	"github.com/linkall-labs/vanus/proto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial("127.0.0.1:11811", opts...)
	if err != nil {
		//eventlog.Fatal(err.Error(), nil)
	}
	cli := segment.NewSegmentServerClient(conn)
	TestAppend(cli)
	conn.Close()
}

func TestAppend(cli segment.SegmentServerClient) {
	id := uint64(1646980924864482000)
	cnt := 0
	for idx := 0; idx < 100; idx++ {
		cnt++
		_, err := cli.AppendToBlock(context.Background(), &segment.AppendToBlockRequest{
			BlockId: id,
			Events: &v1.CloudEventBatch{
				Events: []*v1.CloudEvent{
					{
						Id:          "asdsadsadasdsadsadasdsadsad",
						Source:      "asdsadsadasdsadsadasdsadsad",
						SpecVersion: "1.0",
						Type:        "test1",
						Attributes:  nil,
						Data:        &v1.CloudEvent_TextData{TextData: fmt.Sprintf("Hello DingJie %d", idx)},
					},
				},
			},
		})
		if err != nil {
			//eventlog.Fatal(err.Error(), map[string]interface{}{
			//	"sent": cnt,
			//})
		}
	}
}

func TestRead() {

}
