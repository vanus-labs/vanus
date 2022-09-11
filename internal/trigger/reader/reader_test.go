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
	"math/rand"
	"sync"
	"testing"
	"time"

	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetOffsetByTimestamp(t *testing.T) {
	Convey("test get offset by timestamp", t, func() {
		events := make(chan info.EventRecord, 10)
		r := NewReader(Config{}, events).(*reader)
		eventLogID := vanus.NewID()
		r.elReader = map[vanus.ID]string{eventLogID: "test"}
		rand.Seed(time.Now().Unix())
		offset := rand.Uint64()
		gostub.Stub(&eb.LookupLogOffset, func(ctx context.Context, vrn string, ts int64) (int64, error) {
			return int64(offset), nil
		})
		offsets, err := r.GetOffsetByTimestamp(context.Background(), time.Now().Unix())
		So(err, ShouldBeNil)
		So(len(offsets), ShouldEqual, 1)
		So(offsets[0].EventLogID, ShouldEqual, eventLogID)
		So(offsets[0].Offset, ShouldEqual, offset)
	})
}

func TestGetOffset(t *testing.T) {
	Convey("test get offset", t, func() {
		events := make(chan info.EventRecord, 10)
		r := NewReader(Config{}, events).(*reader)
		eventLogID := vanus.NewID()
		vrn := "test"
		r.elReader = map[vanus.ID]string{eventLogID: vrn}
		Convey("test latest", func() {
			r.config.OffsetType = primitive.LatestOffset
			rand.Seed(time.Now().Unix())
			offset := rand.Uint64()
			gostub.Stub(&eb.LookupLatestLogOffset, func(ctx context.Context, vrn string) (int64, error) {
				return int64(offset), nil
			})
			v, err := r.getOffset(context.Background(), eventLogID, vrn)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, offset)
		})
		Convey("test earliest", func() {
			r.config.OffsetType = primitive.EarliestOffset
			rand.Seed(time.Now().Unix())
			offset := rand.Uint64()
			gostub.Stub(&eb.LookupEarliestLogOffset, func(ctx context.Context, vrn string) (int64, error) {
				return int64(offset), nil
			})
			v, err := r.getOffset(context.Background(), eventLogID, vrn)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, offset)
		})
		Convey("test timestamp", func() {
			r.config.OffsetType = primitive.Timestamp
			r.config.OffsetTimestamp = time.Now().Unix()
			rand.Seed(time.Now().Unix())
			offset := rand.Uint64()
			gostub.Stub(&eb.LookupLogOffset, func(ctx context.Context, vrn string, ts int64) (int64, error) {
				return int64(offset), nil
			})
			v, err := r.getOffset(context.Background(), eventLogID, vrn)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, offset)
		})
		Convey("test exist", func() {
			rand.Seed(time.Now().Unix())
			offset := rand.Uint64()
			r.config.Offset = map[vanus.ID]uint64{eventLogID: offset}
			v, err := r.getOffset(context.Background(), eventLogID, vrn)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, offset)
		})
	})
}

func TestReaderStart(t *testing.T) {
	vrn := "vanus:///eventlog/1?eventbus=1"
	Convey("test start eventLogs", t, func() {
		offset := int64(100)
		gostub.Stub(&eb.LookupLatestLogOffset, func(_ context.Context, _ string) (int64, error) {
			return offset, nil
		})
		gostub.Stub(&eb.LookupReadableLogs, func(_ context.Context, _ string) ([]*record.EventLog, error) {
			return []*record.EventLog{{VRN: vrn, Mode: record.PremRead}}, nil
		})

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		logReader := eventlog.NewMockLogReader(ctrl)
		gostub.Stub(&eb.OpenLogReader, func(_ context.Context, _ string, _ ...eventlog.Option) (eventlog.LogReader, error) {
			return logReader, nil
		})
		index := uint64(offset)
		logReader.EXPECT().Seek(gomock.Any(), gomock.Any(), gomock.Any()).Return(int64(100), nil)
		logReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, size int16) ([]*ce.Event, error) {
				time.Sleep(time.Millisecond)
				e := ce.NewEvent()
				e.SetID(uuid.NewString())
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, index)
				e.SetExtension(eb.XVanusLogOffset, buf)
				index++
				return []*ce.Event{&e}, nil
			})
		logReader.EXPECT().Close(gomock.Any()).Return()
		eventCh := make(chan info.EventRecord, 100)
		r := NewReader(Config{EventBusName: "test"}, eventCh).(*reader)
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
