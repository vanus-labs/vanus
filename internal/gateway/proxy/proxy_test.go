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

package proxy

import (
	stdCtx "context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"testing"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestControllerProxy_GetEvent(t *testing.T) {
	Convey("test get event", t, func() {
		cp := NewControllerProxy(Config{
			Endpoints: []string{"127.0.0.1:20001",
				"127.0.0.1:20002", "127.0.0.1:20003"},
			ProxyPort:              18082,
			CloudEventReceiverPort: 18080,
			Credentials:            insecure.NewCredentials(),
		})

		ctrl := gomock.NewController(t)
		mockClient := client.NewMockClient(ctrl)
		cp.client = mockClient
		utEB1 := api.NewMockEventbus(ctrl)
		utEB2 := api.NewMockEventbus(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(ctx stdCtx.Context, eb string) api.Eventbus {
				switch eb {
				case "ut1":
					return utEB1
				case "ut2":
					return utEB2
				default:
					return nil
				}
			})

		Convey("test invalid params", func() {
			res, err := cp.GetEvent(stdCtx.Background(), &proxypb.GetEventRequest{
				Offset: 0,
				Number: 1,
			})
			So(res, ShouldBeNil)
			So(err, ShouldEqual, errInvalidEventbus)
		})

		reader := api.NewMockBusReader(ctrl)
		Convey("test get 1 event, expect 1", func() {
			el := api.NewMockEventlog(ctrl)
			utEB1.EXPECT().ListLog(gomock.Any()).Times(2).Return([]api.Eventlog{el}, nil)
			id := vanus.NewTestID().Uint64()

			utEB1.EXPECT().Reader(gomock.Any()).Times(2).DoAndReturn(func(
				opts ...api.ReadOption) api.BusReader {
				So(opts, ShouldHaveLength, 3)
				opt := &api.ReadOptions{}
				opt.Apply(opts...)
				So(opt.Policy, ShouldResemble, policy.NewManuallyReadPolicy(el, 0))
				So(opt.BatchSize, ShouldEqual, 1)
				So(opt.PollingTimeout, ShouldEqual, 0)
				return reader
			})

			e1 := v2.NewEvent()
			e1.SetID("ut")
			e1.SetSource("ut")
			e1.SetType("ut")
			e1.SetSpecVersion("1.0")
			e1.SetExtension("123", 123)
			err := e1.SetData(v2.ApplicationJSON, map[string]interface{}{
				"int":    123,
				"bool":   true,
				"string": "string",
			})
			So(err, ShouldBeNil)
			reader.EXPECT().Read(gomock.Any()).Times(2).Return([]*v2.Event{&e1}, int64(0), id, nil)
			res, err := cp.GetEvent(stdCtx.Background(), &proxypb.GetEventRequest{
				Eventbus: "ut1",
				Offset:   -123,
				Number:   1,
			})

			So(err, ShouldBeNil)
			So(res.Events, ShouldHaveLength, 1)

			res, err = cp.GetEvent(stdCtx.Background(), &proxypb.GetEventRequest{
				Eventbus: "ut1",
				Offset:   0,
				Number:   1,
			})

			So(err, ShouldBeNil)
			So(res.Events, ShouldHaveLength, 1)
			data, err := e1.MarshalJSON()
			So(err, ShouldBeNil)
			So(res.Events[0].Value, ShouldResemble, data)
		})

		Convey("test get events, but number exceeded", func() {
			el := api.NewMockEventlog(ctrl)
			utEB1.EXPECT().ListLog(gomock.Any()).Times(1).Return([]api.Eventlog{el}, nil)
			id := vanus.NewTestID().Uint64()

			utEB1.EXPECT().Reader(gomock.Any()).Times(1).DoAndReturn(func(
				opts ...api.ReadOption) api.BusReader {
				So(opts, ShouldHaveLength, 3)
				opt := &api.ReadOptions{}
				opt.Apply(opts...)
				So(opt.Policy, ShouldResemble, policy.NewManuallyReadPolicy(el, 1234))
				So(opt.BatchSize, ShouldEqual, maximumNumberPerGetRequest)
				So(opt.PollingTimeout, ShouldEqual, 0)
				return reader
			})

			events := make([]*v2.Event, maximumNumberPerGetRequest)
			for idx := 0; idx < maximumNumberPerGetRequest; idx++ {
				e := v2.NewEvent()
				e.SetID(fmt.Sprintf("ut%d", idx))
				e.SetSource("ut")
				e.SetType("ut")
				e.SetSpecVersion("1.0")
				e.SetExtension("123", 123)
				err := e.SetData(v2.ApplicationJSON, map[string]interface{}{
					"int":    123,
					"bool":   true,
					"string": "string",
				})
				So(err, ShouldBeNil)
				events[idx] = &e
			}

			reader.EXPECT().Read(gomock.Any()).Times(1).Return(events, int64(1234), id, nil)
			res, err := cp.GetEvent(stdCtx.Background(), &proxypb.GetEventRequest{
				Eventbus: "ut1",
				Offset:   1234,
				Number:   128,
			})

			So(err, ShouldBeNil)
			So(res.Events, ShouldHaveLength, maximumNumberPerGetRequest)
			for idx := range events {
				data, err := events[idx].MarshalJSON()
				So(err, ShouldBeNil)
				So(res.Events[idx].Value, ShouldResemble, data)
			}
		})

		Convey("test get events by eventID", func() {
			b := make([]byte, 16)
			id := vanus.NewTestID().Uint64()
			offset := int64(1234)
			binary.BigEndian.PutUint64(b[0:8], id)
			binary.BigEndian.PutUint64(b[8:], uint64(offset))

			el := api.NewMockEventlog(ctrl)
			utEB1.EXPECT().GetLog(gomock.Any(), id).Times(1).Return(el, nil)
			utEB1.EXPECT().Reader(gomock.Any()).Times(1).DoAndReturn(func(
				opts ...api.ReadOption) api.BusReader {
				So(opts, ShouldHaveLength, 2)
				opt := &api.ReadOptions{}
				opt.Apply(opts...)
				So(opt.Policy, ShouldResemble, policy.NewManuallyReadPolicy(el, offset))
				So(opt.PollingTimeout, ShouldEqual, 0)
				return reader
			})

			e := v2.NewEvent()
			e.SetID("ut")
			e.SetSource("ut")
			e.SetType("ut")
			e.SetSpecVersion("1.0")
			e.SetExtension("123", 123)
			err := e.SetData(v2.ApplicationJSON, map[string]interface{}{
				"int":    123,
				"bool":   true,
				"string": "string",
			})
			So(err, ShouldBeNil)

			reader.EXPECT().Read(gomock.Any()).Times(1).Return([]*v2.Event{&e}, offset, id, nil)
			res, err := cp.GetEvent(stdCtx.Background(), &proxypb.GetEventRequest{
				Eventbus: "ut1",
				EventId:  base64.StdEncoding.EncodeToString(b),
			})

			So(err, ShouldBeNil)
			So(res.Events, ShouldHaveLength, 1)
			data, err := e.MarshalJSON()
			So(err, ShouldBeNil)
			So(res.Events[0].Value, ShouldResemble, data)
		})
	})
}

func TestControllerProxy_StartAndStop(t *testing.T) {
	Convey("test server start and stop", t, func() {
		cp := NewControllerProxy(Config{
			Endpoints: []string{"127.0.0.1:20001",
				"127.0.0.1:20002", "127.0.0.1:20003"},
			CloudEventReceiverPort: 18080,
			ProxyPort:              18082,
			Credentials:            insecure.NewCredentials(),
		})

		err := cp.Start()
		So(err, ShouldBeNil)
		defer cp.Stop()

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		conn, err := grpc.Dial("127.0.0.1:18082", opts...)
		So(err, ShouldBeNil)

		cli := proxypb.NewControllerProxyClient(conn)
		res, err := cli.ClusterInfo(stdCtx.Background(), &emptypb.Empty{})
		So(err, ShouldBeNil)
		So(res.CloudeventsPort, ShouldEqual, 18080)
		So(res.ProxyPort, ShouldEqual, 18082)
	})
}

func TestControllerProxy_LookLookupOffset(t *testing.T) {
	Convey("test lookup offset by timestamp/earliest/latest", t, func() {

	})
}
