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

package reader

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventlog"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"

	"github.com/vanus-labs/vanus/internal/trigger/info"
)

func TestReaderStart(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockClient := client.NewMockClient(mockCtrl)
	mockEventbus := api.NewMockEventbus(mockCtrl)
	mockEventlog := api.NewMockEventlog(mockCtrl)
	mockBusWriter := api.NewMockBusWriter(mockCtrl)
	mockBusReader := api.NewMockBusReader(mockCtrl)
	mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
	mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
	mockEventbus.EXPECT().Reader(Any(), Any()).AnyTimes().Return(mockBusReader)
	mockEventbus.EXPECT().GetLog(Any(), Any()).AnyTimes().Return(mockEventlog, nil)
	mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
	mockEventlog.EXPECT().ID().AnyTimes().Return(uint64(0))

	Convey("test start eventlogs", t, func() {
		offset := int64(100)
		index := uint64(offset)
		mockEventlog.EXPECT().LatestOffset(Any()).AnyTimes().Return(offset, nil)
		mockEventlog.EXPECT().EarliestOffset(Any()).AnyTimes().Return(offset, nil)
		mockBusReader.EXPECT().Read(Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, opts ...api.ReadOption) (*cloudevents.CloudEventBatch, int64, uint64, error) {
				time.Sleep(time.Millisecond)
				e := ce.NewEvent()
				e.SetID(uuid.NewString())
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, index)
				e.SetExtension(eventlog.XVanusLogOffset, buf)
				index++
				epb, _ := codec.ToProto(&e)
				return &cloudevents.CloudEventBatch{
					Events: []*cloudevents.CloudEvent{epb},
				}, int64(0), uint64(0), nil
				// return []*ce.Event{&e}, int64(0), uint64(0), nil
			})
		eventCh := make(chan info.EventRecord, 100)
		r := NewReader(Config{EventbusID: vanus.NewTestID(), BatchSize: 1}, eventCh).(*reader)
		r.config.Client = mockClient
		r.Start()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range eventCh {
				if e.Offset >= uint64(offset+100) {
					return
				}
			}
		}()
		wg.Wait()
		r.Close()
	})
}
