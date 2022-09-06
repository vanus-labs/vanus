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
	"net/url"
	"testing"
	"time"

	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	"github.com/linkall-labs/vanus/internal/primitive"

	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	. "github.com/golang/mock/gomock"
	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
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
	ga := &ceGateway{}
	Convey("test start receive ", t, func() {
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		err := ga.StartReceive(ctx)
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

	Convey("test receive failure2", t, func() {
		e := ce.NewEvent()
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/gateway/test",
			},
		}
		e.SetExtension(primitive.XVanusDeliveryTime, "2006-01-02T15:04:05")
		stub := StubFunc(&requestDataFromContext, reqData)
		defer stub.Reset()
		_, ret := ga.receive(ctx, e)
		So(ret, ShouldBeError)
	})

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

func TestGateway_checkExtension(t *testing.T) {
	Convey("test check extensions", t, func() {
		e := ce.NewEvent()
		err := checkExtension(e.Extensions())
		So(err, ShouldBeNil)
		e.SetExtension(primitive.XVanusDeliveryTime, "test")
		err = checkExtension(e.Extensions())
		So(err, ShouldBeNil)
		e.SetExtension(primitive.XVanus+"fortest", "test")
		err = checkExtension(e.Extensions())
		So(err, ShouldNotBeNil)
	})
}
func TestGateway_getEventBusFromPath(t *testing.T) {
	Convey("test get eventbus from path return nil ", t, func() {
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/test",
			},
		}
		ret := getEventBusFromPath(reqData)
		So(ret, ShouldEqual, "")
	})
	Convey("test get eventbus from path return path ", t, func() {
		reqData := &cehttp.RequestData{
			URL: &url.URL{
				Opaque: "/gateway/test",
			},
		}
		ret := getEventBusFromPath(reqData)
		So(ret, ShouldEqual, "test")
	})
}

func TestGateway_EventID(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()
	var (
		eventID     = "AABBCC"
		busName     = "test"
		controllers = []string{"127.0.0.1:2048"}
		port        = 8080
	)

	writer := eventbus.NewMockBusWriter(ctrl)
	writer.EXPECT().Append(Any(), Any()).Return(eventID, nil)

	stub := StubFunc(&eb.OpenBusWriter, writer, nil)
	defer stub.Reset()

	cfg := Config{
		Port:           port,
		ControllerAddr: controllers,
	}
	ga := NewGateway(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ga.StartReceive(ctx)
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
		event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

		ctx := ce.ContextWithTarget(context.Background(), fmt.Sprintf("http://127.0.0.1:%d/gateway/%s", port, busName))
		resEvent, res := c.Request(ctx, event)
		So(ce.IsACK(res), ShouldBeTrue)
		var httpResult *cehttp.Result
		ce.ResultAs(res, &httpResult)
		So(httpResult, ShouldNotBeNil)

		var ed EventData
		err = resEvent.DataAs(&ed)
		So(err, ShouldBeNil)
		So(ed.BusName, ShouldEqual, busName)
		So(ed.EventID, ShouldEqual, eventID)
	})
}
