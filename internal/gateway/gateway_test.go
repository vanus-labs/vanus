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

package gateway

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	. "github.com/golang/mock/gomock"
	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/cluster"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func TestGateway_NewGateway(t *testing.T) {
	Convey("test new gateway ", t, func() {
		c := Config{
			Port:           8080,
			ControllerAddr: []string{"127.0.0.1"},
		}
		ceGa := NewGateway(c)
		So(ceGa.config.Port, ShouldEqual, 8080)
		So(ceGa.config.ControllerAddr[0], ShouldEqual, "127.0.0.1")
	})
}

func TestGateway_StartReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	p := rd.Int31n(60000) + 3000
	ga := &ceGateway{
		config: Config{
			Port: int(p),
		},
	}
	Convey("test start receive ", t, func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		err := ga.startCloudEventsReceiver(ctx)
		So(err, ShouldBeNil)
	})
}

func TestGateway_receive(t *testing.T) {
	ctx := context.Background()
	ga := &ceGateway{}
	Convey("test receive failure1 ", t, func() {
		e := ce.NewEvent()
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/test",
			},
		}
		stub := StubFunc(&requestDataFromContext, reqData)
		defer stub.Reset()
		_, ret := ga.receive(ctx, e)
		So(ret, ShouldBeError)
	})

	// Convey("test receive failure2", t, func() {
	//	e := ce.NewEvent()
	//	reqData := &cehttp.RequestData{
	//		URL: &url.URL{
	//			Opaque: "/gateway/test",
	//		},
	//	}
	//	e.SetExtension(primitive.XVanusDeliveryTime, "2006-01-02T15:04:05")
	//	stub := StubFunc(&requestDataFromContext, reqData)
	//	defer stub.Reset()
	//	_, ret := ga.receive(ctx, e)
	//	So(ret, ShouldBeError)
	// })

	// Convey("test receive failure3", t, func() {
	// 	e := ce.NewEvent()
	// 	reqData := &cehttp.RequestData{
	// 		URL: &url.URL{
	// 			Opaque: "/gateway/test",
	// 		},
	// 	}
	// 	e.SetExtension(xceVanusDeliveryTime, "2006-01-02T15:04:05Z")
	// 	stub := StubFunc(&requestDataFromContext, reqData)
	// 	defer stub.Reset()
	// 	ga.config = Config{
	// 		ControllerAddr: []string{"127.0.0.1"},
	// 	}
	// 	ret := ga.receive(ctx, e)
	// 	So(ret, ShouldBeError)
	// })
}

func TestGateway_getEventbusFromPath(t *testing.T) {
	ga := &ceGateway{}
	Convey("invalid request path", t, func() {
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/a/b/c/d",
			},
		}
		_, err := ga.getEventbusFromPath(context.Background(), reqData)
		So(err.Error(), ShouldEqual, "invalid request path")

		reqData = &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/ns/b/eb/d",
			},
		}
		_, err = ga.getEventbusFromPath(context.Background(), reqData)
		So(err.Error(), ShouldEqual, "invalid request path")
	})

	Convey("test namespace or eventbus is empty", t, func() {
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/namespaces//eventbus/test/events",
			},
		}
		_, err := ga.getEventbusFromPath(context.Background(), reqData)
		So(err.Error(), ShouldEqual, "namespace is empty")

		reqData = &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/namespaces/default/eventbus//events",
			},
		}
		_, err = ga.getEventbusFromPath(context.Background(), reqData)
		So(err.Error(), ShouldEqual, "eventbus is empty")
	})

	ctrl := NewController(t)
	cctrl := cluster.NewMockCluster(ctrl)
	ebSvc := cluster.NewMockEventbusService(ctrl)
	cctrl.EXPECT().EventbusService().AnyTimes().Return(ebSvc)

	ebID := vanus.NewTestID().Uint64()
	ebSvc.EXPECT().GetEventbusByName(Any(), "default", "test").Times(1).Return(
		&metapb.Eventbus{
			Name:        "test",
			LogNumber:   1,
			Id:          ebID,
			Description: "desc",
			NamespaceId: vanus.NewTestID().Uint64(),
		}, nil)
	ga.ctrl = cctrl
	Convey("test get eventbus from path return path ", t, func() {
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/namespaces/default/eventbus/test/events",
			},
		}

		id, err := ga.getEventbusFromPath(context.Background(), reqData)
		So(err, ShouldBeNil)
		So(id, ShouldEqual, ebID)
	})
}

func TestGateway_EventID(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()
	var (
		busID       = vanus.NewTestID()
		controllers = []string{"127.0.0.1:2048"}
		port        = 8087
	)

	mockClient := client.NewMockClient(ctrl)
	mockEventbus := api.NewMockEventbus(ctrl)
	mockBusWriter := api.NewMockBusWriter(ctrl)
	mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
	mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
	mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{"AABBCC"}, nil)

	cfg := Config{
		Port:           port,
		ControllerAddr: controllers,
	}
	ga := NewGateway(cfg)

	ga.proxySrv.SetClient(mockClient)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ga.startCloudEventsReceiver(ctx)

	time.Sleep(50 * time.Millisecond)

	Convey("test put event and receive response event", t, func() {
		p, err := ce.NewHTTP()
		So(err, ShouldBeNil)
		c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
		So(err, ShouldBeNil)

		event := ce.NewEvent()
		event.SetID("example-event")
		event.SetSource("example/uri")
		event.SetType("example.type")
		_ = event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

		ctx := ce.ContextWithTarget(context.Background(),
			fmt.Sprintf("http://127.0.0.1:%d/gateway/%s", cfg.GetCloudEventReceiverPort(), busID))
		resEvent, res := c.Request(ctx, event)
		So(ce.IsACK(res), ShouldBeTrue)
		var httpResult *cehttp.Result
		ce.ResultAs(res, &httpResult)
		So(httpResult, ShouldNotBeNil)

		So(resEvent, ShouldBeNil)
	})
}
