// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file exceptreq compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toreq writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func CreateEventFactory(ctx context.Context, source string, maxPayloadSize int) <-chan *cloudevents.CloudEvent {
	ch := make(chan *cloudevents.CloudEvent, 1024)
	go generateEvents(ctx, source, maxPayloadSize, ch)
	return ch
}

func generateEvents(ctx context.Context, source string, maxSize int, ch chan *cloudevents.CloudEvent) {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
		default:
			ch <- &cloudevents.CloudEvent{
				Id:          uuid.NewString(),
				Source:      source,
				SpecVersion: "1.0",
				Type:        source,
				Data:        &cloudevents.CloudEvent_TextData{TextData: genStr(rd, maxSize)},
				Attributes: map[string]*cloudevents.CloudEvent_CloudEventAttributeValue{
					"time": {
						Attr: &cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp{
							CeTimestamp: timestamppb.New(time.Now()),
						},
					},
				},
			}
		}
	}
}

func genStr(rd *rand.Rand, maxSize int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	data := ""
	size := int(rd.Int31n(int32(maxSize)) + 8)
	for idx := 0; idx < size; idx++ {
		data = fmt.Sprintf("%s%c", data, str[rd.Int31n(int32(len(str)))])
	}
	return data
}
