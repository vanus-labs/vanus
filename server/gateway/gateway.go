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
	// standard libraries.
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	// third-party libraries.
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	"github.com/vanus-labs/vanus/api/cloudevents"
	"github.com/vanus-labs/vanus/api/cluster"
	proxypb "github.com/vanus-labs/vanus/api/proxy"
	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/tracing"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/server/gateway/proxy"
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

	e, err := cloudevents.ToProto(&event)
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
	httpRequestPrefix = "/gateway"
)

func (ga *ceGateway) getEventbusFromPath(ctx context.Context, reqData *cehttp.RequestData) (vanus.ID, error) {
	// TODO validate
	reqPathStr := reqData.URL.String()
	var (
		ns   string
		name string
	)
	if strings.HasPrefix(reqPathStr, httpRequestPrefix) { // Deprecated, just for compatibility of older than v0.7.0
		ns = primitive.DefaultNamespace
		name = strings.TrimLeft(reqPathStr[len(httpRequestPrefix):], "/")
	} else {
		// namespaces/:namespace_name/eventbus/:eventbus_name/events
		path := strings.TrimLeft(reqData.URL.String(), "/")
		strs := strings.Split(path, "/")
		if len(strs) != 5 {
			return 0, errors.New("invalid request path")
		}
		if strs[0] != "namespaces" && strs[2] != "eventbus" && strs[4] != "events" {
			return 0, errors.New("invalid request path")
		}
		ns = strs[1]
		name = strs[3]
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
