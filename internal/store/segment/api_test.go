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
	"testing"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/errors"
)

func TestSegmentServer(t *testing.T) {
	Convey("Test SegmentServer", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		srv := NewMockServer(ctrl)
		ss := segmentServer{
			srv: srv,
		}

		Convey("Start()", func() {
			srv.EXPECT().Start(Any()).Return(nil)

			req := &segpb.StartSegmentServerRequest{
				ServerId: 1,
			}
			resp, err := ss.Start(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)
		})

		Convey("Stop()", func() {
			srv.EXPECT().Stop(Any()).Return(nil)

			req := &segpb.StopSegmentServerRequest{}
			resp, err := ss.Stop(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)
		})

		Convey("Status()", func() {
			srv.EXPECT().Status().Return(primitive.ServerStateRunning)

			req := &emptypb.Empty{}
			resp, err := ss.Status(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp.GetStatus(), ShouldEqual, primitive.ServerStateRunning)
		})

		Convey("CreateBlock()", func() {
			srv.EXPECT().CreateBlock(Any(), Not(vanus.EmptyID()), Not(0)).Return(nil)
			srv.EXPECT().CreateBlock(Any(), Eq(vanus.EmptyID()), Any()).Return(errors.ErrInvalidRequest)
			srv.EXPECT().CreateBlock(Any(), Any(), Eq(int64(0))).Return(errors.ErrInvalidRequest)

			req := &segpb.CreateBlockRequest{
				Id:   vanus.NewID().Uint64(),
				Size: 4 * 1024 * 1024,
			}
			resp, err := ss.CreateBlock(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)

			req = &segpb.CreateBlockRequest{
				Id:   vanus.NewID().Uint64(),
				Size: 0,
			}
			_, err = ss.CreateBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)

			req = &segpb.CreateBlockRequest{
				Id:   0,
				Size: 4 * 1024 * 1024,
			}
			_, err = ss.CreateBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)
		})

		Convey("RemoveBlock()", func() {
			srv.EXPECT().RemoveBlock(Any(), Not(vanus.EmptyID())).Return(nil)
			srv.EXPECT().RemoveBlock(Any(), Eq(vanus.EmptyID())).Return(errors.ErrInvalidRequest)

			req := &segpb.RemoveBlockRequest{
				Id: vanus.NewID().Uint64(),
			}
			resp, err := ss.RemoveBlock(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)

			req = &segpb.RemoveBlockRequest{
				Id: 0,
			}
			_, err = ss.RemoveBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)
		})

		Convey("GetBlockInfo()", func() {
			// FIXME: not implement.
			req := &segpb.GetBlockInfoRequest{}
			resp, err := ss.GetBlockInfo(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)
		})

		Convey("ActivateSegment()", func() {
			// TODO(james.yin):
			srv.EXPECT().ActivateSegment(Any(), Any(), Any(), Any()).Return(nil)

			req := &segpb.ActivateSegmentRequest{
				EventLogId:     vanus.NewID().Uint64(),
				ReplicaGroupId: vanus.NewID().Uint64(),
				Replicas: map[uint64]string{
					1: "127.0.0.1:11811",
				},
			}
			resp, err := ss.ActivateSegment(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp, ShouldNotBeNil)
		})

		Convey("InactivateSegment()", func() {
			// TODO(james.yin):
			// srv.EXPECT().InactivateSegment(Any(), Any()).Return(nil)

			req := &segpb.InactivateSegmentRequest{}
			_, err := ss.InactivateSegment(context.Background(), req)
			So(err, ShouldBeNil)
		})

		Convey("AppendToBlock()", func() {
			srv.EXPECT().AppendToBlock(Any(), Not(vanus.EmptyID()), Not(Len(0))).Return([]int64{1}, nil)
			srv.EXPECT().AppendToBlock(Any(), Eq(vanus.EmptyID()), Any()).Return(nil, errors.ErrInvalidRequest)
			srv.EXPECT().AppendToBlock(Any(), Any(), Len(0)).Return(nil, errors.ErrInvalidRequest)

			req := &segpb.AppendToBlockRequest{
				BlockId: vanus.NewID().Uint64(),
				Events: &cepb.CloudEventBatch{
					Events: make([]*cepb.CloudEvent, 1),
				},
			}
			resp, err := ss.AppendToBlock(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp.Offsets, ShouldResemble, []int64{1})

			req = &segpb.AppendToBlockRequest{
				BlockId: 0,
				Events: &cepb.CloudEventBatch{
					Events: make([]*cepb.CloudEvent, 1),
				},
			}
			_, err = ss.AppendToBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)

			req = &segpb.AppendToBlockRequest{
				BlockId: vanus.NewID().Uint64(),
				Events:  &cepb.CloudEventBatch{},
			}
			_, err = ss.AppendToBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)
		})

		Convey("ReadFromBlock()", func() {
			id := vanus.NewID()
			srv.EXPECT().ReadFromBlock(Any(), Not(vanus.EmptyID()), Any(), Not(0), Any()).Return(make([]*cepb.CloudEvent, 1), nil)
			srv.EXPECT().ReadFromBlock(Any(), Eq(vanus.EmptyID()), Any(), Any(), Any()).Return(nil, errors.ErrInvalidRequest)
			srv.EXPECT().ReadFromBlock(Any(), Any(), Any(), Eq(0), Any()).Return(nil, errors.ErrResourceNotFound)

			req := &segpb.ReadFromBlockRequest{
				BlockId: id.Uint64(),
				Number:  1,
			}
			resp, err := ss.ReadFromBlock(context.Background(), req)
			So(err, ShouldBeNil)
			So(resp.GetEvents().GetEvents(), ShouldResemble, []*cepb.CloudEvent{nil})

			req = &segpb.ReadFromBlockRequest{
				BlockId: 0,
			}
			_, err = ss.ReadFromBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrInvalidRequest)

			req = &segpb.ReadFromBlockRequest{
				BlockId: vanus.NewID().Uint64(),
			}
			_, err = ss.ReadFromBlock(context.Background(), req)
			So(err, ShouldEqual, errors.ErrResourceNotFound)
		})
	})
}
