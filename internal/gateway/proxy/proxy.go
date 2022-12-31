// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file exceptreq compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toreq writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/option"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/internal/trigger/transform"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/linkall-labs/vanus/pkg/cluster"
	"github.com/linkall-labs/vanus/pkg/errors"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	maximumNumberPerGetRequest = 64
)

var (
	errInvalidEventbus = errors.New("the eventbus name can't be empty")
)

type Config struct {
	Endpoints              []string
	ProxyPort              int
	CloudEventReceiverPort int
	Credentials            credentials.TransportCredentials
	GRPCReflectionEnable   bool
}

var (
	_ cloudevents.CloudEventsServer = &ControllerProxy{}
)

type ControllerProxy struct {
	cfg          Config
	tracer       *tracing.Tracer
	client       eb.Client
	eventbusCtrl ctrlpb.EventBusControllerClient
	eventlogCtrl ctrlpb.EventLogControllerClient
	triggerCtrl  ctrlpb.TriggerControllerClient
	grpcSrv      *grpc.Server
	ctrl         cluster.Cluster
}

func (cp *ControllerProxy) Send(ctx context.Context, batch *cloudevents.BatchEvent) (*emptypb.Empty, error) {
	_ctx, span := cp.tracer.Start(ctx, "Send")
	defer span.End()

	if batch.EventbusName == "" {
		return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid eventbus name")
	}

	for idx := range batch.Events.Events {
		e := batch.Events.Events[idx]
		err := checkExtension(e.Attributes)
		if err != nil {
			return nil, v2.NewHTTPResult(http.StatusBadRequest, err.Error())
		}
		if e.Attributes == nil {
			e.Attributes = make(map[string]*cloudevents.CloudEvent_CloudEventAttributeValue, 0)
		}
		e.Attributes[primitive.XVanusEventbus] = &cloudevents.CloudEvent_CloudEventAttributeValue{
			Attr: &cloudevents.CloudEvent_CloudEventAttributeValue_CeString{CeString: batch.EventbusName},
		}
		if eventTime, ok := e.Attributes[primitive.XVanusDeliveryTime]; ok {
			// validate event time
			if _, err := types.ParseTime(eventTime.String()); err != nil {
				log.Error(_ctx, "invalid format of event time", map[string]interface{}{
					log.KeyError: err,
					"eventTime":  eventTime.String(),
				})
				return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid delivery time")
			}
			// TODO process delay message
			// ebName = primitive.TimerEventbusName
		}
	}

	err := cp.client.Eventbus(ctx, batch.GetEventbusName()).Writer().AppendBatch(_ctx, batch.GetEvents())
	if err != nil {
		log.Warning(_ctx, "append to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   batch.EventbusName,
		})
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}

	return &emptypb.Empty{}, nil
}

func checkExtension(extensions map[string]*cloudevents.CloudEvent_CloudEventAttributeValue) error {
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

func NewControllerProxy(cfg Config) *ControllerProxy {
	ctrl := cluster.NewClusterController(cfg.Endpoints, insecure.NewCredentials())
	return &ControllerProxy{
		cfg:          cfg,
		ctrl:         ctrl,
		client:       eb.Connect(cfg.Endpoints),
		tracer:       tracing.NewTracer("controller-proxy", trace.SpanKindServer),
		eventbusCtrl: ctrl.EventbusService().RawClient(),
		eventlogCtrl: ctrl.EventlogService().RawClient(),
		triggerCtrl:  ctrl.TriggerService().RawClient(),
	}
}

func (cp *ControllerProxy) Start() error {
	recoveryOpt := recovery.WithRecoveryHandlerContext(
		func(ctx context.Context, p interface{}) error {
			log.Error(ctx, "goroutine panicked", map[string]interface{}{
				log.KeyError: fmt.Sprintf("%v", p),
				"stack":      string(debug.Stack()),
			})
			return status.Errorf(codes.Internal, "%v", p)
		},
	)

	cp.grpcSrv = grpc.NewServer(
		grpc.ChainStreamInterceptor(
			errinterceptor.StreamServerInterceptor(),
			recovery.StreamServerInterceptor(recoveryOpt),
			otelgrpc.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			errinterceptor.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recoveryOpt),
			otelgrpc.UnaryServerInterceptor(),
		),
	)

	// for debug in developing stage
	if cp.cfg.GRPCReflectionEnable {
		reflection.Register(cp.grpcSrv)
	}

	proxypb.RegisterControllerProxyServer(cp.grpcSrv, cp)
	cloudevents.RegisterCloudEventsServer(cp.grpcSrv, cp)

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cp.cfg.ProxyPort))
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = cp.grpcSrv.Serve(listen)
		if err != nil {
			panic(fmt.Sprintf("start grpc proxy failed: %s", err.Error()))
		}
		wg.Done()
	}()
	log.Info(context.Background(), "the grpc proxy ready to work", nil)
	return nil
}

func (cp *ControllerProxy) Stop() {
	if cp.grpcSrv != nil {
		cp.grpcSrv.GracefulStop()
	}
}

func (cp *ControllerProxy) ClusterInfo(_ context.Context, _ *emptypb.Empty) (*proxypb.ClusterInfoResponse, error) {
	return &proxypb.ClusterInfoResponse{
		CloudeventsPort: int64(cp.cfg.CloudEventReceiverPort),
		ProxyPort:       int64(cp.cfg.ProxyPort),
	}, nil
}

func (cp *ControllerProxy) LookupOffset(ctx context.Context,
	req *proxypb.LookupOffsetRequest) (*proxypb.LookupOffsetResponse, error) {
	elList := make([]api.Eventlog, 0)
	if req.EventlogId > 0 {
		id := vanus.NewIDFromUint64(req.EventlogId)
		l, err := cp.client.Eventbus(ctx, req.GetEventbus()).GetLog(ctx, id.Uint64())
		if err != nil {
			return nil, err
		}
		elList = append(elList, l)
	} else {
		ls, err := cp.client.Eventbus(ctx, req.GetEventbus()).ListLog(ctx)
		if err != nil {
			return nil, err
		}
		elList = ls
	}
	if len(elList) == 0 {
		return nil, errors.New("eventbus not found")
	}
	res := &proxypb.LookupOffsetResponse{
		Offsets: map[uint64]int64{},
	}
	for idx := range elList {
		l := elList[idx]
		off, err := l.QueryOffsetByTime(ctx, req.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to lookup offset: %w", err)
		}
		res.Offsets[l.ID()] = off
	}
	return res, nil
}

func (cp *ControllerProxy) GetEvent(ctx context.Context,
	req *proxypb.GetEventRequest) (*proxypb.GetEventResponse, error) {
	if req.GetEventbus() == "" {
		return nil, errInvalidEventbus
	}

	if req.EventId != "" {
		return cp.getByEventID(ctx, req)
	}

	var (
		offset = req.Offset
		num    = req.Number
	)

	if offset < 0 {
		offset = 0
	}

	if num > maximumNumberPerGetRequest {
		num = maximumNumberPerGetRequest
	}

	ls, err := cp.client.Eventbus(ctx, req.GetEventbus()).ListLog(ctx)
	if err != nil {
		return nil, err
	}

	events, _, _, err := cp.client.Eventbus(ctx, req.GetEventbus()).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], offset)),
		option.WithBatchSize(int(num)),
	).Read(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]*wrapperspb.BytesValue, len(events))
	for idx, v := range events {
		data, _ := v.MarshalJSON()
		results[idx] = wrapperspb.Bytes(data)
	}
	return &proxypb.GetEventResponse{
		Events: results,
	}, nil
}

func (cp *ControllerProxy) ValidateSubscription(ctx context.Context,
	req *proxypb.ValidateSubscriptionRequest) (*proxypb.ValidateSubscriptionResponse, error) {
	if req.GetEvent() == nil {
		res, err := cp.GetEvent(ctx, &proxypb.GetEventRequest{
			Eventbus:   req.Eventbus,
			EventlogId: req.Eventlog,
			Offset:     req.Offset,
			Number:     1,
		})
		if err != nil {
			return nil, err
		}
		req.Event = res.GetEvents()[0].Value
	}

	e := v2.NewEvent()
	if err := e.UnmarshalJSON(req.GetEvent()); err != nil {
		return nil, errors.ErrInvalidRequest.WithMessage("failed to unmarshall event to CloudEvent").Wrap(err)
	}

	if req.GetSubscription() == nil {
		sub, err := cp.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: req.SubscriptionId})
		if err != nil {
			return nil, err
		}
		req.Subscription = &ctrlpb.SubscriptionRequest{
			Filters:     sub.Filters,
			Transformer: sub.Transformer,
		}
	}

	sub := convert.FromPbSubscriptionRequest(req.Subscription)
	res := &proxypb.ValidateSubscriptionResponse{}
	f := filter.GetFilter(sub.Filters)
	r := f.Filter(e)
	if !r {
		return res, nil
	}

	res.FilterResult = true
	t := transform.NewTransformer(sub.Transformer)
	if t != nil {
		if err := t.Execute(&e); err != nil {
			return nil, errors.ErrTransformInputParse.Wrap(err)
		}
	}
	data, _ := e.MarshalJSON()
	res.TransformerResult = data
	return res, nil
}

// getByEventID why added this? can it be deleted?
func (cp *ControllerProxy) getByEventID(ctx context.Context,
	req *proxypb.GetEventRequest) (*proxypb.GetEventResponse, error) {
	logID, off, err := decodeEventID(req.EventId)
	if err != nil {
		return nil, err
	}

	l, err := cp.client.Eventbus(ctx, req.GetEventbus()).GetLog(ctx, logID)
	if err != nil {
		return nil, err
	}

	events, _, _, err := cp.client.Eventbus(ctx, req.GetEventbus()).Reader(
		option.WithReadPolicy(policy.NewManuallyReadPolicy(l, off)),
		option.WithDisablePolling(),
	).Read(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]*wrapperspb.BytesValue, len(events))
	for idx, v := range events {
		data, _ := v.MarshalJSON()
		results[idx] = wrapperspb.Bytes(data)
	}
	return &proxypb.GetEventResponse{
		Events: results,
	}, nil
}

func decodeEventID(eventID string) (uint64, int64, error) {
	decoded, err := base64.StdEncoding.DecodeString(eventID)
	if err != nil {
		return 0, 0, err
	}
	if len(decoded) != 16 { // fixed length
		return 0, 0, fmt.Errorf("invalid event id")
	}
	logID := binary.BigEndian.Uint64(decoded[0:8])
	off := binary.BigEndian.Uint64(decoded[8:16])
	return logID, int64(off), nil
}
