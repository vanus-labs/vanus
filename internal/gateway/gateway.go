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
	"errors"
	"fmt"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
	"go.opentelemetry.io/otel/trace"
	"net"
	"net/http"
	"strings"
	"sync"

	eb "github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/tracing"

	"github.com/vanus-labs/vanus/internal/gateway/proxy"
)

const (
	httpRequestPrefix = "/gateway"
)

var requestDataFromContext = cehttp.RequestDataFromContext

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
	eventbusId, err := getEventbusFromPath(requestDataFromContext(ctx))
	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}

	e, err := codec.ToProto(&event)
	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}

	_, err = ga.proxySrv.Publish(ctx, &proxypb.PublishRequest{
		Events: &cloudevents.CloudEventBatch{
			Events: []*cloudevents.CloudEvent{e},
		},
		EventbusId: eventbusId.Uint64(),
	})

	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}

	return re, v2.ResultACK
}

func getEventbusFromPath(reqData *cehttp.RequestData) (vanus.ID, error) {
	// TODO validate
	reqPathStr := reqData.URL.String()
	if !strings.HasPrefix(reqPathStr, httpRequestPrefix) {
		return vanus.EmptyID(), errors.New("invalid eventbus id")
	}
	id, err := vanus.NewIDFromString(strings.TrimLeft(reqPathStr[len(httpRequestPrefix):], "/"))
	if err != nil {
		return vanus.EmptyID(), err
	}

	return id, nil
}
