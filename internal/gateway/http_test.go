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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/go-resty/resty/v2"
	"github.com/linkall-labs/vanus/client"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHTTP(t *testing.T) {
	event := ce.NewEvent()
	event.SetID("example-event")
	event.SetSource("example/uri")
	event.SetType("example.type")
	event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})
	stub1 := gostub.StubFunc(&client.SearchEventByID, &event, nil)
	defer stub1.Reset()

	cfg := Config{
		Port:           8080,
		ControllerAddr: []string{"127.0.0.1:2048"},
	}

	go MustStartHTTP(cfg)
	time.Sleep(500 * time.Millisecond)
	httpClient := resty.New()

	Convey("test get events by event id", t, func() {
		res, err := httpClient.NewRequest().Get(fmt.Sprintf("%s%d/getEvents?eventid=%s", "http://127.0.0.1:", cfg.Port+1, "AABBCC"))
		So(err, ShouldBeNil)
		So(res.StatusCode(), ShouldEqual, http.StatusOK)
		data := new(struct {
			Events []ce.Event
		})
		err = json.Unmarshal(res.Body(), data)
		So(err, ShouldBeNil)
		So(data.Events[0], ShouldResemble, event)
	})

}
