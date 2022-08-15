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
	"net"
	"net/http"
	"strings"
	"sync"

	"encoding/json"

	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/observability/log"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/cloudevents/sdk-go/v2/types"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
)

const (
	httpRequestPrefix    = "/gateway"
	xceVanusEventbus     = "xvanuseventbus"
	xceVanusDeliveryTime = "xvanusdeliverytime"
	timerBuiltInEventbus = "__Timer_RS"
	ctrlProxyPortShift   = 2
)

var (
	allowCtrlProxyList = map[string]string{
		"/linkall.vanus.controller.PingServer/Ping":                      "ALLOW",
		"/linkall.vanus.controller.EventBusController/ListEventBus":      "ALLOW",
		"/linkall.vanus.controller.EventBusController/CreateEventBus":    "ALLOW",
		"/linkall.vanus.controller.EventBusController/DeleteEventBus":    "ALLOW",
		"/linkall.vanus.controller.EventBusController/GetEventBus":       "ALLOW",
		"/linkall.vanus.controller.EventLogController/ListSegment":       "ALLOW",
		"/linkall.vanus.controller.TriggerController/CreateSubscription": "ALLOW",
		"/linkall.vanus.controller.TriggerController/DeleteSubscription": "ALLOW",
		"/linkall.vanus.controller.TriggerController/GetSubscription":    "ALLOW",
		"/linkall.vanus.controller.TriggerController/ListSubscription":   "ALLOW",
	}
)

var (
	requestDataFromContext = cehttp.RequestDataFromContext
)

type EventData struct {
	EventID string
	BusName string
}

type ceGateway struct {
	// ceClient  v2.Client
	busWriter sync.Map
	config    Config
	cp        *ctrlProxy
}

func NewGateway(config Config) *ceGateway {
	return &ceGateway{
		config: config,
		cp:     newCtrlProxy(config.Port+ctrlProxyPortShift, allowCtrlProxyList, config.ControllerAddr),
	}
}

func (ga *ceGateway) StartCtrlProxy(ctx context.Context) error {
	return ga.cp.start(ctx)
}

func (ga *ceGateway) StartReceive(ctx context.Context) error {
	ls, err := net.Listen("tcp", fmt.Sprintf(":%d", ga.config.Port))
	if err != nil {
		return err
	}

	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		return err
	}
	return c.StartReceiver(ctx, ga.receive)
}

func (ga *ceGateway) receive(ctx context.Context, event v2.Event) (*v2.Event, protocol.Result) {
	ebName := getEventBusFromPath(requestDataFromContext(ctx))

	if ebName == "" {
		return nil, fmt.Errorf("invalid eventbus name")
	}

	event.SetExtension(xceVanusEventbus, ebName)

	extensions := event.Extensions()
	if eventTime, ok := extensions[xceVanusDeliveryTime]; ok {
		// validate event time
		if _, err := types.ParseTime(eventTime.(string)); err != nil {
			log.Error(ctx, "invalid format of event time", map[string]interface{}{
				log.KeyError: err,
				"eventTime":  eventTime.(string),
			})
			return nil, fmt.Errorf("invalid delivery time")
		}
		ebName = timerBuiltInEventbus
	}

	vrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s", ga.config.ControllerAddr[0],
		ebName, strings.Join(ga.config.ControllerAddr, ","))
	v, exist := ga.busWriter.Load(vrn)
	if !exist {
		writer, err := eb.OpenBusWriter(vrn)
		if err != nil {
			return nil, protocol.Result(err)
		}

		var loaded bool
		v, loaded = ga.busWriter.LoadOrStore(vrn, writer)
		if loaded {
			writer.Close()
		}
	}
	writer, _ := v.(eventbus.BusWriter)
	eventID, err := writer.Append(ctx, &event)
	if err != nil {
		log.Warning(ctx, "append to failed", map[string]interface{}{
			log.KeyError: err,
			"vrn":        vrn,
		})
		return nil, v2.NewHTTPResult(http.StatusBadRequest, err.Error())
	}
	eventData := EventData{
		BusName: ebName,
		EventID: eventID,
	}
	resEvent, err := createResponseEvent(eventData)
	if err != nil {
		return nil, fmt.Errorf("create response event error")
	}
	return resEvent, v2.ResultACK
}

func getEventBusFromPath(reqData *cehttp.RequestData) string {
	// TODO validate
	reqPathStr := reqData.URL.String()
	if !strings.HasPrefix(reqPathStr, httpRequestPrefix) {
		return ""
	}
	return strings.TrimLeft(reqPathStr[len(httpRequestPrefix):], "/")
}

func createResponseEvent(eventData EventData) (*v2.Event, error) {
	e := &v2.Event{}
	e.SetSpecVersion("1.0")
	e.SetType("com.linkall.vanus.event.stored")
	e.SetSource("https://linkall.com/vanus")
	e.SetID(uuid.NewString())

	dataMarshal, err := json.Marshal(eventData)
	if err != nil {
		return nil, err
	}

	e.SetData("Text/plain", dataMarshal)
	return e, nil
}
