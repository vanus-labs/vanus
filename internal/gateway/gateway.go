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
	"net"
	"net/http"
	"strings"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/vanus-labs/vanus/internal/gateway/proxy"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/tracing"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

var requestDataFromContext = cehttp.RequestDataFromContext

type EventData struct {
	EventID string   `json:"event_id"`
	BusID   vanus.ID `json:"eventbus_id"`
}

type ceGateway struct {
	config     Config
	proxySrv   *proxy.ControllerProxy
	tracer     *tracing.Tracer
	ceListener net.Listener
	ctrl       cluster.Cluster
}

func NewGateway(config Config) *ceGateway {
	ctrl := cluster.NewClusterController(config.GetProxyConfig().Endpoints, insecure.NewCredentials())
	return &ceGateway{
		config:   config,
		ctrl:     ctrl,
		proxySrv: proxy.NewControllerProxy(config.GetProxyConfig()),
		tracer:   tracing.NewTracer("cloudevents", trace.SpanKindServer),
	}
}

func (ga *ceGateway) Start(ctx context.Context) error {
	if err := ga.startCloudEventsReceiver(ctx); err != nil {
		return err
	}
	return ga.proxySrv.Start()
}

func (ga *ceGateway) Stop() {
	ga.proxySrv.Stop()
	if err := ga.ceListener.Close(); err != nil {
		log.Warn().Err(err).Msg("close CloudEvents listener error")
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
	eventbusID, err := ga.getEventbusFromPath(ctx, requestDataFromContext(ctx))
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
		EventbusId: eventbusID.Uint64(),
	})

	if err != nil {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}

	return re, v2.ResultACK
}

const (
	httpRequestPrefix = "gateway"
)

func (ga *ceGateway) getEventbusFromPath(ctx context.Context, reqData *cehttp.RequestData) (vanus.ID, error) {
	// TODO validate
	reqPathStr := reqData.URL.String()
	reqPathStr = reqPathStr[1:]
	var (
		ns   string
		name string
	)
	paths := strings.Split(reqPathStr, "/")
	if len(paths) == 2 {
		if paths[0] == httpRequestPrefix { // Deprecated, just for compatibility of older than v0.7.0
			// gateway/eb_name
			ns = primitive.DefaultNamespace
			name = paths[1]
		} else if paths[1] == "events" {
			// eb_id/events
			eventbusID, err := vanus.NewIDFromString(paths[0])
			if err != nil {
				return 0, err
			}
			// check eb exist
			_, err = ga.ctrl.EventbusService().GetEventbus(ctx, eventbusID.Uint64())
			if err != nil {
				return 0, err
			}
			return eventbusID, nil
		} else {
			return 0, errors.New("invalid request path")
		}
	} else if len(paths) == 5 {
		// namespaces/:namespace_name/eventbus/:eventbus_name/events
		if paths[0] != "namespaces" && paths[2] != "eventbus" && paths[4] != "events" {
			return 0, errors.New("invalid request path")
		}
		ns = paths[1]
		name = paths[3]
	} else {
		return 0, errors.New("invalid request path")
	}

	if ns == "" {
		return 0, errors.New("namespace is empty")
	}

	if name == "" {
		return 0, errors.New("eventbus is empty")
	}

	eb, err := ga.ctrl.EventbusService().GetEventbusByName(ctx, ns, name)
	if err != nil {
		return 0, err
	}
	return vanus.NewIDFromUint64(eb.Id), nil
}
