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
	stdCtx "context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/go-resty/resty/v2"
	. "github.com/golang/mock/gomock"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	el "github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHTTP(t *testing.T) {
	event := ce.NewEvent()
	event.SetID("example-event")
	event.SetSource("example/uri")
	event.SetType("example.type")
	event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

	cfg := Config{
		Port:           8080,
		ControllerAddr: []string{"127.0.0.1:2048"},
	}

	go MustStartHTTP(cfg)
	time.Sleep(50 * time.Millisecond)
	httpClient := resty.New()

	Convey("test get events by event id", t, func() {
		stub1 := gostub.StubFunc(&eb.SearchEventByID, &event, nil)
		defer stub1.Reset()
		res, err := httpClient.NewRequest().Get(fmt.Sprintf("http://127.0.0.1:%d/getEvents?eventid=%s", cfg.Port+1, "AABBCC"))
		So(err, ShouldBeNil)
		So(res.StatusCode(), ShouldEqual, http.StatusOK)
		data := new(struct {
			Events []ce.Event
		})
		err = json.Unmarshal(res.Body(), data)
		So(err, ShouldBeNil)
		So(data.Events, ShouldResemble, []ce.Event{event})
	})

	Convey("test get events by eventbus name, offset and num", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()
		r := el.NewMockLogReader(ctrl)
		r.EXPECT().Seek(Any(), Any(), Any()).Return(int64(0), nil)
		r.EXPECT().Read(Any(), Any()).Return([]*ce.Event{&event}, nil)
		r.EXPECT().Close(stdCtx.Background()).Return()

		el := &record.EventLog{}

		stub1 := gostub.StubFunc(&eb.LookupReadableLogs, []*record.EventLog{el}, nil)
		defer stub1.Reset()
		stub2 := gostub.StubFunc(&eb.OpenLogReader, r, nil)
		defer stub2.Reset()

		res, err := httpClient.NewRequest().Get(fmt.Sprintf("http://127.0.0.1:%d/getEvents?eventbus=%s&offset=%d&number=%d",
			cfg.Port+1, "test", 0, 1))
		So(err, ShouldBeNil)
		So(res.StatusCode(), ShouldEqual, http.StatusOK)
		data := new(struct {
			Events []ce.Event
		})
		err = json.Unmarshal(res.Body(), data)
		So(err, ShouldBeNil)
		So(data.Events, ShouldResemble, []ce.Event{event})
	})

	Convey("test getController endpoints", t, func() {
		res, err := httpClient.NewRequest().Get(fmt.Sprintf("http://127.0.0.1:%d/getControllerEndpoints", cfg.Port+1))
		So(err, ShouldBeNil)
		So(res.StatusCode(), ShouldEqual, http.StatusOK)
		data := new(struct {
			Endpoints []string
		})
		err = json.Unmarshal(res.Body(), data)
		So(err, ShouldBeNil)
		So(data.Endpoints, ShouldResemble, cfg.ControllerAddr)
	})
}
