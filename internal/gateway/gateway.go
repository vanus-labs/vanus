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
	"strconv"
	"strings"
	"sync"
	"time"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/google/uuid"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/internal/gateway/proxy"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
)

const (
	httpRequestPrefix = "/gateway"
)

var (
	requestDataFromContext = cehttp.RequestDataFromContext
)

type EventData struct {
	EventID string `json:"event_id"`
	BusName string `json:"eventbus_name"`
}

type ceGateway struct {
	// ceClient  v2.Client
	busWriter  sync.Map
	config     Config
	client     eb.Client
	proxySrv   *proxy.ControllerProxy
	tracer     *tracing.Tracer
	ceListener net.Listener
}

func NewGateway(config Config) *ceGateway {
	return &ceGateway{
		config:   config,
		client:   eb.Connect(config.ControllerAddr),
		proxySrv: proxy.NewControllerProxy(config.GetProxyConfig()),
		tracer:   tracing.NewTracer("cloudevents", trace.SpanKindServer),
	}
}

func (ga *ceGateway) Start(ctx context.Context) error {
	if err := ga.startCloudEventsReceiver(ctx); err != nil {
		return err
	}
	if err := ga.proxySrv.Start(); err != nil {
		return err
	}
	return nil
}

func (ga *ceGateway) Stop() {
	ga.proxySrv.Stop()
	if err := ga.ceListener.Close(); err != nil {
		log.Warning(context.Background(), "close CloudEvents listener error", map[string]interface{}{
			log.KeyError: err,
		})
	}
}

func (ga *ceGateway) startCloudEventsReceiver(ctx context.Context) error {
	ls, err := net.Listen("tcp", fmt.Sprintf(":%d", ga.config.GetCloudEventReceiverPort()))
	if err != nil {
		return err
	}

	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		return err
	}

	ga.ceListener = ls
	go func() {
		if err := c.StartReceiver(ctx, ga.receive); err != nil {
			panic(fmt.Sprintf("start CloudEvents receiver failed: %s", err.Error()))
		}
	}()
	return nil
}

func (ga *ceGateway) receive(ctx context.Context, event v2.Event) (re *v2.Event, result protocol.Result) {
	_ctx, span := ga.tracer.Start(ctx, "receive")

	start := time.Now()
	ebName := getEventBusFromPath(requestDataFromContext(_ctx))
	defer func() {
		used := float64(time.Since(start)) / float64(time.Millisecond)
		metrics.GatewayEventReceivedCountVec.WithLabelValues(
			ebName,
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(1, 10),
			"unknown",
		).Inc()

		metrics.GatewayEventWriteLatencySummaryVec.WithLabelValues(
			ebName,
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(1, 10),
		).Observe(used)
		span.End()
	}()

	if ebName == "" {
		return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid eventbus name")
	}

	extensions := event.Extensions()
	err := checkExtension(extensions)
	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusBadRequest, err.Error())
	}

	event.SetExtension(primitive.XVanusEventbus, ebName)
	if eventTime, ok := extensions[primitive.XVanusDeliveryTime]; ok {
		// validate event time
		if _, err := types.ParseTime(eventTime.(string)); err != nil {
			log.Error(_ctx, "invalid format of event time", map[string]interface{}{
				log.KeyError: err,
				"eventTime":  eventTime.(string),
			})
			return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid delivery time")
		}
		ebName = primitive.TimerEventbusName
	}

	v, exist := ga.busWriter.Load(ebName)
	if !exist {
		v, _ = ga.busWriter.LoadOrStore(ebName, ga.client.Eventbus(ctx, ebName).Writer())
	}
	writer, _ := v.(api.BusWriter)
	eventID, err := writer.AppendOne(_ctx, &event)
	if err != nil {
		log.Warning(_ctx, "append to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   ebName,
		})
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}
	eventData := EventData{
		BusName: ebName,
		EventID: eventID,
	}
	re, err = createResponseEvent(eventData)
	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}
	return re, v2.ResultACK
}

func checkExtension(extensions map[string]interface{}) error {
	if len(extensions) == 0 {
		return nil
	}
	for name := range extensions {
		if name == primitive.XVanusDeliveryTime {
			continue
		}
		// event attribute can not prefix with vanus system use
		if strings.HasPrefix(name, primitive.XVanus) {
			return fmt.Errorf("invalid ce attribute [%s] perfix %s", name, primitive.XVanus)
		}
	}
	return nil
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
	e := v2.NewEvent("1.0")
	e.SetID(uuid.NewString())
	e.SetType("com.linkall.vanus.event.stored")
	e.SetSource("https://linkall.com/vanus")

	err := e.SetData(v2.ApplicationJSON, eventData)
	if err != nil {
		return nil, err
	}
	return &e, nil
}
