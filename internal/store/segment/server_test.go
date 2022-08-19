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

package segment

import (
	// standard libraries.
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/linkall-labs/vanus/proto/pkg/errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
	"github.com/linkall-labs/vanus/internal/util"
)

const (
	shortDelayInTest = 200 * time.Millisecond
	longDelayInTest  = 3 * time.Second
)

func TestServer_RemoveBlock(t *testing.T) {
	Convey("remove block", t, func() {
		srv := &server{
			state: primitive.ServerStateCreated,
		}

		Convey("state checking", func() {
			err := srv.RemoveBlock(context.Background(), vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "service state error")
			So(et.Code, ShouldEqual, errors.ErrorCode_SERVICE_NOT_RUNNING)
			So(et.Message, ShouldEqual, fmt.Sprintf(
				"the server isn't ready to work, current state: %s",
				primitive.ServerStateCreated))
		})

		Convey("not found block", func() {
			srv.state = primitive.ServerStateRunning

			err := srv.RemoveBlock(context.Background(), vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Message, ShouldEqual, "the block not found")
		})

		Convey("delete block", func() {
			ctrl := NewController(t)
			defer ctrl.Finish()

			id := vanus.NewID()
			b := NewMockReplica(ctrl)
			b.EXPECT().ID().AnyTimes().Return(id)
			b.EXPECT().Delete(Any()).Times(1)
			srv.replicas.Store(id, b)

			srv.state = primitive.ServerStateRunning

			err := srv.RemoveBlock(context.Background(), id)
			So(err, ShouldBeNil)
			So(util.MapLen(&srv.replicas), ShouldEqual, 0)
		})
	})
}

func TestServer_ReadFromBlock(t *testing.T) {
	Convey("not found block", t, func() {
		srv := &server{
			state: primitive.ServerStateRunning,
		}

		_, err := srv.ReadFromBlock(context.Background(), vanus.NewID(), 0, 3, false)
		So(err, ShouldNotBeNil)
		So(err.(*errors.ErrorType).Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
	})

	Convey("read from block", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		srv := &server{
			state: primitive.ServerStateRunning,
		}

		id := vanus.NewID()
		b := NewMockReplica(ctrl)
		b.EXPECT().ID().AnyTimes().Return(id)
		b.EXPECT().IDStr().AnyTimes().Return(id.String())
		srv.replicas.Store(id, b)

		ent0 := cetest.MakeStoredEntry0(ctrl)
		ent1 := cetest.MakeStoredEntry1(ctrl)

		Convey("enable long-polling, but not wait", func() {
			b.EXPECT().Read(Any(), int64(0), 3).Return([]block.Entry{ent0, ent1}, nil)

			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), shortDelayInTest)
			defer cancel()

			events, err := srv.ReadFromBlock(ctx, id, 0, 3, true)
			So(time.Now(), ShouldHappenBefore, start.Add(shortDelayInTest))
			So(err, ShouldBeNil)
			So(events, ShouldHaveLength, 2)
			cetest.CheckEvent0(events[0])
			cetest.CheckEvent1(events[1])
		})

		Convey("long-polling without timeout", func() {
			var newMessageArrived uint64
			b.EXPECT().Read(Any(), int64(0), 3).Times(2).DoAndReturn(func(
				ctx context.Context, off int64, num int,
			) ([]block.Entry, error) {
				if atomic.LoadUint64(&newMessageArrived) == 0 {
					return nil, block.ErrOnEnd
				}
				return []block.Entry{ent0, ent1}, nil
			})

			mgr := NewMockpollingManager(ctrl)
			ch := make(chan struct{})
			mgr.EXPECT().Add(Any(), id).Times(1).Return(ch)
			srv.pm = mgr

			start := time.Now()
			go func() {
				time.Sleep(shortDelayInTest)
				atomic.StoreUint64(&newMessageArrived, 1)
				close(ch)
			}()

			ctx, cancel := context.WithTimeout(context.Background(), longDelayInTest)
			defer cancel()

			events, err := srv.ReadFromBlock(ctx, id, 0, 3, true)
			So(time.Now(), ShouldHappenBetween, start.Add(shortDelayInTest), start.Add(longDelayInTest))
			So(err, ShouldBeNil)
			So(events, ShouldHaveLength, 2)
			cetest.CheckEvent0(events[0])
			cetest.CheckEvent1(events[1])
		})

		Convey("long-polling with timeout", func() {
			b.EXPECT().Read(Any(), int64(0), 3).Times(1).Return(nil, block.ErrOnEnd)

			mgr := NewMockpollingManager(ctrl)
			ch := make(chan struct{})
			mgr.EXPECT().Add(Any(), id).Times(1).Return(ch)
			srv.pm = mgr

			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), shortDelayInTest)
			defer cancel()

			_, err := srv.ReadFromBlock(ctx, id, 0, 3, true)
			So(time.Now(), ShouldHappenAfter, start.Add(shortDelayInTest))
			So(err, ShouldBeError, block.ErrOnEnd)
		})

		Convey("long-polling with canceled request", func() {
			b.EXPECT().Read(Any(), int64(0), 3).Times(1).Return(nil, block.ErrOnEnd)

			mgr := NewMockpollingManager(ctrl)
			ch := make(chan struct{})
			mgr.EXPECT().Add(Any(), id).Times(1).Return(ch)
			srv.pm = mgr

			ctx, cancel := context.WithCancel(context.Background())

			start := time.Now()
			go func() {
				time.Sleep(shortDelayInTest)
				cancel()
			}()

			ctx, cancel2 := context.WithTimeout(ctx, longDelayInTest)
			defer cancel2()

			_, err := srv.ReadFromBlock(ctx, id, 0, 3, true)
			So(time.Now(), ShouldHappenBetween, start.Add(shortDelayInTest), start.Add(longDelayInTest))
			So(err, ShouldBeError, context.Canceled)
		})
	})
}
