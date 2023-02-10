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
	v2 "github.com/cloudevents/sdk-go/v2"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

func CreateEventFactory(ctx context.Context, source string, maxPayloadSize int) <-chan *v2.Event {
	ch := make(chan *v2.Event, 1024)
	go generateEvents(ctx, source, maxPayloadSize, ch)
	return ch
}

func generateEvents(ctx context.Context, source string, maxSize int, ch chan *v2.Event) {
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-ctx.Done():
		default:
			e := v2.NewEvent()
			e.SetID(uuid.NewString())
			e.SetSource(source)
			e.SetSpecVersion(v2.VersionV1)
			e.SetType(source)
			_ = e.SetData(v2.TextPlain, genStr(rd, maxSize))
			e.SetTime(time.Now())
			ch <- &e
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
