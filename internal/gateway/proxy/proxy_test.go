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
	stdJson "encoding/json"
	"fmt"
	"testing"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"github.com/linkall-labs/vanus/proto/pkg/codec"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tidwall/gjson"
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
			epb1, _ := codec.ToProto(&e1)
			batchret := &cloudevents.CloudEventBatch{
				Events: []*cloudevents.CloudEvent{epb1},
			}
			reader.EXPECT().Read(gomock.Any()).Times(2).Return(batchret, int64(0), id, nil)
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
			eventpbs := make([]*cloudevents.CloudEvent, maximumNumberPerGetRequest)
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
				epb, _ := codec.ToProto(&e)
				eventpbs[idx] = epb
			}

			ret := &cloudevents.CloudEventBatch{
				Events: eventpbs,
			}
			reader.EXPECT().Read(gomock.Any()).Times(1).Return(ret, int64(1234), id, nil)
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
			epb1, _ := codec.ToProto(&e)
			ret := &cloudevents.CloudEventBatch{
				Events: []*cloudevents.CloudEvent{epb1},
			}
			reader.EXPECT().Read(gomock.Any()).Times(1).Return(ret, offset, id, nil)
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

func TestControllerProxy_ValidateSubscription(t *testing.T) {
	Convey("test ValidateSubscription", t, func() {
		cp := NewControllerProxy(Config{
			Endpoints: []string{"127.0.0.1:20001",
				"127.0.0.1:20002", "127.0.0.1:20003"},
			CloudEventReceiverPort: 18080,
			ProxyPort:              18082,
			Credentials:            insecure.NewCredentials(),
		})

		data := `{
    "id":"13b719a4-ada9-436a-9fb1-fc2bc82dc647",
    "source":"prometheus",
    "specversion":"1.0",
    "type":"naive-http-request",
    "datacontenttype":"application/json",
    "subject":"operator",
    "time":"2022-12-12T08:31:54.936803649Z",
    "data":{"body":{"alerts":[{"annotations":{"feishuUrls":[{"URL":"https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx",
"signature":"yyyy"},{"URL":"https://open.feishu.cn/open-apis/bot/v2/hook/yyyyy","signature":""},
{"URL":"https://open.feishu.cn/open-apis/bot/v2/hook/zzzzz","signature":"zzzz"}]},
"labels":{"forward":"test-server","severity":"P1"},"startsAt":"2022-12-12T07:55:24.893471163Z","status":"resolved"}],
"commonLabels":{"cluster":"test","forward":"test-server","groups":"test-bot","severity":"P1"}},
"headers":{"Content-Type":"application/json","Host":"webhook-source.vanus:80","User-Agent":"Alertmanager/0.24.0"},
"method":"POST","query_args":{"source":"prometheus","subject":"operator"}}
}`
		e := v2.NewEvent()
		_ = e.UnmarshalJSON([]byte(data))

		trans := `{"pipeline":[{"command":["create","$.xvfeishuservice","bot"]},{"command":["create",
				"$.xvfeishumsgtype","interactive"]},{"command":["join","$.xvfeishuboturls",",",
				"$.data.body.alerts[0].annotations.feishuUrls[:].URL"]},{"command":["join",
				"$.xvfeishubotsigns",",","$.data.body.alerts[0].annotations.feishuUrls[:].signature"]}]}`
		var _transformer *primitive.Transformer
		_ = stdJson.Unmarshal([]byte(trans), &_transformer)
		transPb := convert.ToPbTransformer(_transformer)
		s := &ctrlpb.SubscriptionRequest{
			Filters: []*metapb.Filter{
				{
					Exact: map[string]string{
						"source": "test",
					},
				},
			},
			Transformer: transPb,
		}
		Convey("test with event and subscription", func() {
			ctx := stdCtx.Background()
			res, err := cp.ValidateSubscription(ctx, &proxypb.ValidateSubscriptionRequest{
				Event:        []byte(data),
				Subscription: s,
			})
			So(err, ShouldBeNil)
			So(res.FilterResult, ShouldBeFalse)

			s.Filters = []*metapb.Filter{
				{
					Exact: map[string]string{
						"source": "prometheus",
					},
				},
			}
			res, err = cp.ValidateSubscription(ctx, &proxypb.ValidateSubscriptionRequest{
				Event:        []byte(data),
				Subscription: s,
			})
			So(err, ShouldBeNil)
			So(res.FilterResult, ShouldBeTrue)
			result := gjson.ParseBytes(res.TransformerResult)
			urls := "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx,https://open" +
				".feishu.cn/open-apis/bot/v2/hook/yyyyy,https://open.feishu.cn/open-apis/bot/v2/hook/zzzzz"
			So(result.Get("xvfeishumsgtype").String(), ShouldEqual, "interactive")
			So(result.Get("xvfeishuboturls").String(), ShouldEqual, urls)
			So(result.Get("xvfeishubotsigns").String(), ShouldEqual, "yyyy,,zzzz")
			So(result.Get("xvfeishuservice").String(), ShouldEqual, "bot")
		})

		ctrl := gomock.NewController(t)
		cli := client.NewMockClient(ctrl)
		cp.client = cli
		eb := api.NewMockEventbus(ctrl)
		mockTriggerCtrl := ctrlpb.NewMockTriggerControllerClient(ctrl)
		cp.triggerCtrl = mockTriggerCtrl
		Convey("test with eventlog, offset and subscriptionID", func() {
			ctx := stdCtx.Background()

			// mock eventbus
			cli.EXPECT().Eventbus(gomock.Any(), gomock.Any()).Times(2).Return(eb)
			eb.EXPECT().ListLog(gomock.Any()).Times(1).Return([]api.Eventlog{nil}, nil)
			rd := api.NewMockBusReader(ctrl)
			eb.EXPECT().Reader(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(rd)
			epb, _ := codec.ToProto(&e)
			ret := &cloudevents.CloudEventBatch{
				Events: []*cloudevents.CloudEvent{epb},
			}
			rd.EXPECT().Read(gomock.Any()).Times(1).Return(ret, int64(0), uint64(0), nil)

			// mock subscription
			pb := &metapb.Subscription{
				Filters: []*metapb.Filter{
					{
						Exact: map[string]string{
							"source": "prometheus",
						},
					},
				},
				Transformer: s.Transformer,
			}
			mockTriggerCtrl.EXPECT().GetSubscription(ctx, gomock.Any()).Times(1).Return(pb, nil)

			res, err := cp.ValidateSubscription(ctx, &proxypb.ValidateSubscriptionRequest{
				SubscriptionId: vanus.NewTestID().Uint64(),
				Eventbus:       "test",
				Eventlog:       vanus.NewTestID().Uint64(),
				Offset:         123,
			})

			So(err, ShouldBeNil)
			So(res.FilterResult, ShouldBeTrue)
			result := gjson.ParseBytes(res.TransformerResult)
			urls := "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxx,https://open" +
				".feishu.cn/open-apis/bot/v2/hook/yyyyy,https://open.feishu.cn/open-apis/bot/v2/hook/zzzzz"
			So(result.Get("xvfeishumsgtype").String(), ShouldEqual, "interactive")
			So(result.Get("xvfeishuboturls").String(), ShouldEqual, urls)
			So(result.Get("xvfeishubotsigns").String(), ShouldEqual, "yyyy,,zzzz")
			So(result.Get("xvfeishuservice").String(), ShouldEqual, "bot")
		})
	})
}
