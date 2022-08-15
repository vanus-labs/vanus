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
	"net/url"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"

	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestUtils_NewGateway(t *testing.T) {
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

func TestUtils_StartReceive(t *testing.T) {
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

func TestUtils_receive(t *testing.T) {
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
		e.SetExtension(xceVanusDeliveryTime, "2006-01-02T15:04:05")
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

func TestUtils_getEventBusFromPath(t *testing.T) {
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
