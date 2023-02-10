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
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	stdtime "time"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/cloudevents/sdk-go/v2/types"
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/codec"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/option"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/internal/trigger/transform"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
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
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	maximumNumberPerGetRequest = 64
	eventChanCache             = 10
	readSize                   = 5
	ContentTypeProtobuf        = "application/protobuf"
	httpRequestPrefix          = "/gatewaysink"
	datacontenttype            = "datacontenttype"
	dataschema                 = "dataschema"
	subject                    = "subject"
	time                       = "time"
)

var (
	errInvalidEventbus     = errors.New("the eventbus name can't be empty")
	requestDataFromContext = cehttp.RequestDataFromContext
	zeroTime               = stdtime.Time{}
)

type Config struct {
	Endpoints              []string
	SinkPort               int
	ProxyPort              int
	CloudEventReceiverPort int
	Credentials            credentials.TransportCredentials
	GRPCReflectionEnable   bool
}

var (
	_ proxypb.StoreProxyServer = &ControllerProxy{}
)

type ackCallback func(bool)

type message struct {
	sequenceID uint64
	event      *v2.Event
}

type subscribeCache struct {
	sequenceID      uint64
	subscriptionID  string
	subscribeStream proxypb.StoreProxy_SubscribeServer
	acks            sync.Map
	eventc          chan message
}

func newSubscribeCache(subscriptionID string, stream proxypb.StoreProxy_SubscribeServer) *subscribeCache {
	return &subscribeCache{
		sequenceID:      0,
		subscriptionID:  subscriptionID,
		subscribeStream: stream,
		acks:            sync.Map{},
		eventc:          make(chan message, eventChanCache),
	}
}

func (s *subscribeCache) ch() chan message {
	return s.eventc
}

func (s *subscribeCache) stream() proxypb.StoreProxy_SubscribeServer {
	return s.subscribeStream
}

type ControllerProxy struct {
	cfg          Config
	tracer       *tracing.Tracer
	client       eb.Client
	eventbusCtrl ctrlpb.EventBusControllerClient
	eventlogCtrl ctrlpb.EventLogControllerClient
	triggerCtrl  ctrlpb.TriggerControllerClient
	grpcSrv      *grpc.Server
	ctrl         cluster.Cluster
	writerMap    sync.Map
	cache        sync.Map
}

func (cp *ControllerProxy) GetDeadLetterEvent(ctx context.Context,
	req *proxypb.GetDeadLetterEventRequest) (*proxypb.GetDeadLetterEventResponse, error) {
	if req.GetSubscriptionId() == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("subscription is empty")
	}

	subscription, err := cp.triggerCtrl.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: req.GetSubscriptionId()})
	if err != nil {
		return nil, err
	}
	storeOffset, err := cp.triggerCtrl.GetDeadLetterEventOffset(ctx, &ctrlpb.GetDeadLetterEventOffsetRequest{
		SubscriptionId: req.SubscriptionId,
	})
	if err != nil {
		return nil, err
	}
	var (
		offset = req.Offset
		num    = req.Number
	)
	if offset == 0 {
		offset = storeOffset.GetOffset()
	} else if offset < storeOffset.GetOffset() {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("offset is invalid, param is %d it but now is %d", offset, storeOffset.Offset),
		)
	}

	if num > maximumNumberPerGetRequest {
		num = maximumNumberPerGetRequest
	}
	deadLetterEventbusName := primitive.GetDeadLetterEventbusName(subscription.EventBus)
	ls, err := cp.client.Eventbus(ctx, deadLetterEventbusName).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	readPolicy := policy.NewManuallyReadPolicy(ls[0], int64(offset))
	busReader := cp.client.Eventbus(ctx, deadLetterEventbusName).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(readPolicy),
		option.WithBatchSize(int(num)),
	)
	subscriptionIDStr := vanus.NewIDFromUint64(req.SubscriptionId).String()
	var events []*v2.Event
loop:
	for {
		_events, _, _, err := busReader.Read(ctx)
		if err != nil {
			if errors.Is(err, errors.ErrOffsetOnEnd) {
				// read end
				break
			}
			// todo some error need retry read
			return nil, err
		}
		if len(_events) == 0 {
			break
		}
		for _, v := range _events {
			ec, _ := v.Context.(*v2.EventContextV1)
			eventSubID := ec.Extensions[primitive.XVanusSubscriptionID]
			if eventSubID != subscriptionIDStr {
				continue
			}
			events = append(events, v)
			if len(events) == int(num) {
				break loop
			}
		}
		readPolicy.Forward(len(_events))
	}
	results := make([]*wrapperspb.BytesValue, len(events))
	for idx, v := range events {
		data, _ := v.MarshalJSON()
		results[idx] = wrapperspb.Bytes(data)
	}
	return &proxypb.GetDeadLetterEventResponse{
		Events: results,
	}, nil
}

func (cp *ControllerProxy) ResendDeadLetterEvent(ctx context.Context,
	req *proxypb.ResendDeadLetterEventRequest) (*emptypb.Empty, error) {
	if req.GetSubscriptionId() == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("subscription is empty")
	}
	subscription, err := cp.triggerCtrl.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: req.GetSubscriptionId()})
	if err != nil {
		return nil, err
	}
	subscriptionIDStr := vanus.NewIDFromUint64(req.SubscriptionId).String()
	storeOffset, err := cp.triggerCtrl.GetDeadLetterEventOffset(ctx, &ctrlpb.GetDeadLetterEventOffsetRequest{
		SubscriptionId: req.SubscriptionId,
	})
	if err != nil {
		return nil, err
	}
	offset := req.GetStartOffset()
	if offset == 0 {
		offset = storeOffset.GetOffset()
	} else if offset < storeOffset.GetOffset() {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("start_offset is invalid, param is %d it but now is %d", offset, storeOffset.Offset),
		)
	}
	deadLetterEventbusName := primitive.GetDeadLetterEventbusName(subscription.EventBus)
	ls, err := cp.client.Eventbus(ctx, deadLetterEventbusName).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	readPolicy := policy.NewManuallyReadPolicy(ls[0], int64(offset))
	busReader := cp.client.Eventbus(ctx, deadLetterEventbusName).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(readPolicy),
		option.WithBatchSize(readSize),
	)
	if req.GetEndOffset() != 0 && req.GetEndOffset() < offset {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("end_offset is invalid, param is %d it but start is %d", offset, offset),
		)
	}
	var endOffset uint64
	var events []*cloudevents.CloudEvent
loop:
	for {
		_events, _, _, err := busReader.Read(ctx)
		if err != nil {
			if errors.Is(err, errors.ErrOffsetOnEnd) {
				// read end
				break
			}
			// todo errors.ErrTryAgain maybe need retry read
			return nil, err
		}
		if len(_events) == 0 {
			break
		}
		for _, v := range _events {
			ec, _ := v.Context.(*v2.EventContextV1)
			offsetByte, _ := ec.Extensions[eventlog.XVanusLogOffset].([]byte)
			_endOffset := binary.BigEndian.Uint64(offsetByte)
			if req.GetEndOffset() != 0 && _endOffset > req.GetEndOffset() {
				break loop
			}
			endOffset = _endOffset
			eventSubID := ec.Extensions[primitive.XVanusSubscriptionID]
			if eventSubID != subscriptionIDStr {
				continue
			}
			// remove retry attribute
			delete(ec.Extensions, primitive.XVanusRetryAttempts)
			// remove dead letter attribute
			delete(ec.Extensions, primitive.LastDeliveryTime)
			delete(ec.Extensions, primitive.LastDeliveryError)
			delete(ec.Extensions, primitive.DeadLetterReason)
			pbEvent, err := codec.ToProto(v)
			if err != nil {
				return nil, err
			}
			events = append(events, pbEvent)
		}
		readPolicy.Forward(len(_events))
		if len(events) > 10 {
			err = cp.writeDeadLetterEvent(ctx, req.SubscriptionId, endOffset+1, events)
			if err != nil {
				return nil, err
			}
			events = nil
		}
	}
	if len(events) > 0 {
		err = cp.writeDeadLetterEvent(ctx, req.SubscriptionId, endOffset+1, events)
		if err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControllerProxy) writeDeadLetterEvent(ctx context.Context,
	subscriptionID uint64,
	offset uint64,
	events []*cloudevents.CloudEvent) error {
	// write to retry eventbus
	err := cp.writeEvents(ctx, primitive.GetRetryEventbusName(""), &cloudevents.CloudEventBatch{Events: events})
	if err != nil {
		return errors.ErrInternal.Wrap(err).WithMessage("write event error")
	}
	// save offset
	_, err = cp.triggerCtrl.SetDeadLetterEventOffset(ctx, &ctrlpb.SetDeadLetterEventOffsetRequest{
		SubscriptionId: subscriptionID, Offset: offset,
	})
	if err != nil {
		return errors.ErrInternal.Wrap(err).WithMessage("save offset error")
	}
	return nil
}

func (cp *ControllerProxy) Publish(ctx context.Context, req *proxypb.PublishRequest) (*emptypb.Empty, error) {
	if req.EventbusName == "" {
		return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid eventbus name")
	}

	_ctx, span := cp.tracer.Start(ctx, "Publish")
	start := stdtime.Now()
	defer func() {
		span.End()
		used := float64(stdtime.Since(start)) / float64(stdtime.Millisecond)
		metrics.GatewayEventReceivedCountVec.WithLabelValues(
			req.EventbusName,
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(int64(len(req.Events.Events)), 10),
			"unknown",
		).Inc()

		metrics.GatewayEventWriteLatencySummaryVec.WithLabelValues(
			req.EventbusName,
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(int64(len(req.Events.Events)), 10),
		).Observe(used)
	}()

	for idx := range req.Events.Events {
		e := req.Events.Events[idx]
		err := checkExtension(e.Attributes)
		if err != nil {
			return nil, v2.NewHTTPResult(http.StatusBadRequest, err.Error())
		}
		if e.Attributes == nil {
			e.Attributes = make(map[string]*cloudevents.CloudEvent_CloudEventAttributeValue, 0)
		}
		e.Attributes[primitive.XVanusEventbus] = &cloudevents.CloudEvent_CloudEventAttributeValue{
			Attr: &cloudevents.CloudEvent_CloudEventAttributeValue_CeString{CeString: req.EventbusName},
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

	err := cp.writeEvents(ctx, req.EventbusName, req.Events)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControllerProxy) writeEvents(ctx context.Context,
	eventbusName string,
	events *cloudevents.CloudEventBatch) error {
	val, exist := cp.writerMap.Load(eventbusName)
	if !exist {
		val, _ = cp.writerMap.LoadOrStore(eventbusName, cp.client.Eventbus(ctx, eventbusName).Writer())
	}
	w, _ := val.(api.BusWriter)
	err := w.AppendBatch(ctx, events)
	if err != nil {
		log.Warning(ctx, "append to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   eventbusName,
		})
		return err
	}
	return nil
}

func (cp *ControllerProxy) Subscribe(req *proxypb.SubscribeRequest, stream proxypb.StoreProxy_SubscribeServer) error {
	_ctx, span := cp.tracer.Start(context.Background(), "Subscribe")
	defer span.End()

	// 1. modify subscription sink
	subscriptionID, err := vanus.NewIDFromString(req.SubscriptionId)
	if err != nil {
		log.Error(_ctx, "parse subscription id failed", map[string]interface{}{
			log.KeyError: err,
			"id":         req.SubscriptionId,
		})
		return err
	}

	getSubscriptionReq := &ctrlpb.GetSubscriptionRequest{
		Id: subscriptionID.Uint64(),
	}
	meta, err := cp.triggerCtrl.GetSubscription(context.Background(), getSubscriptionReq)
	if err != nil {
		log.Error(_ctx, "get subscription failed", map[string]interface{}{
			log.KeyError: err,
			"id":         req.SubscriptionId,
		})
		return err
	}

	newSink := fmt.Sprintf("http://%s:%d%s/%s",
		os.Getenv("POD_IP"), cp.cfg.SinkPort, httpRequestPrefix, req.SubscriptionId)
	if meta.Sink != newSink {
		updateSubscriptionReq := &ctrlpb.UpdateSubscriptionRequest{
			Id: subscriptionID.Uint64(),
			Subscription: &ctrlpb.SubscriptionRequest{
				Source:      meta.Source,
				Types:       meta.Types,
				Config:      meta.Config,
				Filters:     meta.Filters,
				Sink:        newSink,
				Protocol:    meta.Protocol,
				EventBus:    meta.EventBus,
				Transformer: meta.Transformer,
				Name:        meta.Name,
				Description: meta.Description,
				Disable:     meta.Disable,
			},
		}
		_, err = cp.triggerCtrl.UpdateSubscription(_ctx, updateSubscriptionReq)
		if err != nil {
			log.Error(_ctx, "update subscription sink failed", map[string]interface{}{
				log.KeyError: err,
				"id":         req.SubscriptionId,
			})
			return err
		}
	}

	// 2. cache subscribe info
	subscribe := newSubscribeCache(req.SubscriptionId, stream)
	cp.cache.Store(req.SubscriptionId, subscribe)

	// 3. receive and forward events
	for {
		select {
		case <-_ctx.Done():
			return errors.ErrInternal.WithMessage("subscribe stream context done")
		case msg := <-subscribe.ch():
			eventpb, err := ToProto(msg.event)
			if err != nil {
				// TODO(jiangkai): err check
				log.Error(_ctx, "to eventpb failed", map[string]interface{}{
					log.KeyError: err,
					"event":      msg.event,
				})
				break
			}
			log.Debug(_ctx, "subscribe stream send event", map[string]interface{}{
				log.KeyError: err,
				"eventpb":    eventpb.String(),
			})
			err = subscribe.stream().Send(&proxypb.SubscribeResponse{
				SequenceId: msg.sequenceID,
				Events: &cloudevents.CloudEventBatch{
					Events: []*cloudevents.CloudEvent{eventpb},
				},
			})
			if err != nil {
				cache, _ := cp.cache.LoadAndDelete(subscribe.subscriptionID)
				if cache != nil {
					cache.(*subscribeCache).acks.Range(func(key, value interface{}) bool {
						value.(ackCallback)(false)
						return true
					})
				}
				return err
			}
		}
	}
}

func (cp *ControllerProxy) Ack(stream proxypb.StoreProxy_AckServer) error {
	_ctx, span := cp.tracer.Start(context.Background(), "Ack")
	defer span.End()
	for {
		rsp, err := stream.Recv()
		if err != nil {
			log.Error(_ctx, "ack stream recv failed", map[string]interface{}{
				log.KeyError: err,
			})
			return err
		}
		log.Debug(_ctx, "ack stream recv a response", map[string]interface{}{
			log.KeyError: err,
			"rsp":        rsp,
		})
		cache, ok := cp.cache.Load(rsp.SubscriptionId)
		if !ok {
			// TODO(jiangkai): err check
			log.Error(_ctx, "subscription not found", map[string]interface{}{
				log.KeyError:      err,
				"subscription-id": rsp.SubscriptionId,
			})
			continue
		}
		cb, _ := cache.(*subscribeCache).acks.LoadAndDelete(rsp.SequenceId)
		if cb != nil {
			cb.(ackCallback)(rsp.Success)
		}
	}
}

func ToProto(e *v2.Event) (*cloudevents.CloudEvent, error) {
	container := &cloudevents.CloudEvent{
		Id:          e.ID(),
		Source:      e.Source(),
		SpecVersion: e.SpecVersion(),
		Type:        e.Type(),
		Attributes:  make(map[string]*cloudevents.CloudEvent_CloudEventAttributeValue),
	}
	if e.DataContentType() != "" {
		container.Attributes[datacontenttype], _ = attributeFor(e.DataContentType())
	}
	if e.DataSchema() != "" {
		container.Attributes[dataschema], _ = attributeFor(e.DataSchema())
	}
	if e.Subject() != "" {
		container.Attributes[subject], _ = attributeFor(e.Subject())
	}
	if e.Time() != zeroTime {
		container.Attributes[time], _ = attributeFor(e.Time())
	}
	for name, value := range e.Extensions() {
		attr, err := attributeFor(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode attribute %s: %w", name, err)
		}
		container.Attributes[name] = attr
	}
	container.Data = &cloudevents.CloudEvent_BinaryData{
		BinaryData: e.Data(),
	}
	if e.DataContentType() == ContentTypeProtobuf {
		anymsg := &anypb.Any{
			TypeUrl: e.DataSchema(),
			Value:   e.Data(),
		}
		container.Data = &cloudevents.CloudEvent_ProtoData{
			ProtoData: anymsg,
		}
	}
	return container, nil
}

func attributeFor(v interface{}) (*cloudevents.CloudEvent_CloudEventAttributeValue, error) {
	vv, err := types.Validate(v)
	if err != nil {
		return nil, err
	}
	attr := &cloudevents.CloudEvent_CloudEventAttributeValue{}
	switch vt := vv.(type) {
	case bool:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeBoolean{
			CeBoolean: vt,
		}
	case int32:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeInteger{
			CeInteger: vt,
		}
	case string:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeString{
			CeString: vt,
		}
	case []byte:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeBytes{
			CeBytes: vt,
		}
	case types.URI:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeUri{
			CeUri: vt.String(),
		}
	case types.URIRef:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeUriRef{
			CeUriRef: vt.String(),
		}
	case types.Timestamp:
		attr.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp{
			CeTimestamp: timestamppb.New(vt.Time),
		}
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", v)
	}
	return attr, nil
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
	proxypb.RegisterStoreProxyServer(cp.grpcSrv, cp)

	proxyListen, err := net.Listen("tcp", fmt.Sprintf(":%d", cp.cfg.ProxyPort))
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = cp.grpcSrv.Serve(proxyListen)
		if err != nil {
			panic(fmt.Sprintf("start grpc proxy failed: %s", err.Error()))
		}
		wg.Done()
	}()
	log.Info(context.Background(), "the grpc proxy ready to work", nil)

	sinkListen, err := net.Listen("tcp", fmt.Sprintf(":%d", cp.cfg.SinkPort))
	if err != nil {
		return err
	}

	c, err := client.NewHTTP(cehttp.WithListener(sinkListen), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		if err := c.StartReceiver(context.Background(), cp.receive); err != nil {
			panic(fmt.Sprintf("start CloudEvents receiver failed: %s", err.Error()))
		}
		wg.Done()
	}()
	log.Info(context.Background(), "the sink proxy ready to work", nil)
	return nil
}

func (cp *ControllerProxy) receive(ctx context.Context, event v2.Event) (*v2.Event, protocol.Result) {
	_ctx, span := cp.tracer.Start(ctx, "receive")
	defer span.End()
	subscriptionID := getSubscriptionIDFromPath(requestDataFromContext(_ctx))
	if subscriptionID == "" {
		return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid subscription id")
	}
	cache, ok := cp.cache.Load(subscriptionID)
	if !ok {
		// retry
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, "subscription not exist")
	}
	log.Debug(_ctx, "sink proxy received a event", map[string]interface{}{
		"event": event.String(),
	})
	sequenceID := atomic.AddUint64(&cache.(*subscribeCache).sequenceID, 1)
	var success bool
	donec := make(chan struct{})
	cache.(*subscribeCache).acks.Store(sequenceID, ackCallback(func(result bool) {
		log.Info(_ctx, "ack callback", map[string]interface{}{
			"result": result,
		})
		success = result
		close(donec)
	}))
	cache.(*subscribeCache).eventc <- message{
		sequenceID: sequenceID,
		event:      &event,
	}
	<-donec

	if !success {
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, "event processing failed")
	}
	return nil, v2.ResultACK
}

func getSubscriptionIDFromPath(reqData *cehttp.RequestData) string {
	// TODO validate
	reqPathStr := reqData.URL.String()
	if !strings.HasPrefix(reqPathStr, httpRequestPrefix) {
		return ""
	}
	return strings.TrimLeft(reqPathStr[len(httpRequestPrefix):], "/")
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
