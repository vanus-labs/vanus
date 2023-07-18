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

package proxy

import (
	// standard libraries.
	"context"
	"encoding/base64"
	"encoding/binary"
	stderr "errors"
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

	// third-party libraries.
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/cloudevents/sdk-go/v2/types"
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
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

	// first-party libraries.
	eb "github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/observability/tracing"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"

	// third-party libraries.
	"github.com/vanus-labs/vanus/internal/convert"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/trigger/filter"
	"github.com/vanus-labs/vanus/internal/trigger/transform"
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
	eventbusCtrl ctrlpb.EventbusControllerClient
	eventlogCtrl ctrlpb.EventlogControllerClient
	triggerCtrl  ctrlpb.TriggerControllerClient
	nsCtrl       ctrlpb.NamespaceControllerClient
	grpcSrv      *grpc.Server
	ctrl         cluster.Cluster
	writerMap    sync.Map
	cache        sync.Map
}

// Make sure ControllerProxy implements proxypb.StoreProxyServer.
var _ proxypb.StoreProxyServer = (*ControllerProxy)(nil)

func (cp *ControllerProxy) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	eventbusID := vanus.NewIDFromUint64(req.EventbusId)
	responseCode := 200
	_ctx, span := cp.tracer.Start(ctx, "Publish")
	start := stdtime.Now()
	defer func() {
		span.End()
		used := float64(stdtime.Since(start)) / float64(stdtime.Millisecond)
		metrics.GatewayEventReceivedCountVec.WithLabelValues(
			eventbusID.Key(),
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(int64(len(req.Events.Events)), 10),
			strconv.Itoa(responseCode),
		).Add(float64(len(req.Events.Events)))

		metrics.GatewayEventWriteLatencySummaryVec.WithLabelValues(
			eventbusID.Key(),
			metrics.LabelValueProtocolHTTP,
			strconv.FormatInt(int64(len(req.Events.Events)), 10),
		).Observe(used)
	}()

	// todo  common event with delay event mixture
	for idx := range req.Events.Events {
		e := req.Events.Events[idx]
		err := checkExtension(e.Attributes)
		if err != nil {
			responseCode = http.StatusBadRequest
			return nil, v2.NewHTTPResult(http.StatusBadRequest, err.Error())
		}
		if e.Attributes == nil {
			e.Attributes = make(map[string]*cloudevents.CloudEvent_CloudEventAttributeValue, 0)
		}
		e.Attributes[primitive.XVanusEventbus] = &cloudevents.CloudEvent_CloudEventAttributeValue{
			Attr: &cloudevents.CloudEvent_CloudEventAttributeValue_CeString{CeString: eventbusID.Key()},
		}
		if eventTime, ok := e.Attributes[primitive.XVanusDeliveryTime]; ok {
			switch attr := eventTime.GetAttr().(type) {
			case *cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp:
			case *cloudevents.CloudEvent_CloudEventAttributeValue_CeString:
				// validate event time
				t, err := types.ParseTime(attr.CeString)
				if err != nil {
					log.Error(_ctx, "invalid format of event time", map[string]interface{}{
						log.KeyError: err,
						"eventTime":  eventTime.String(),
					})
					responseCode = http.StatusBadRequest
					return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid delivery time")
				}
				eventTime.Attr = &cloudevents.CloudEvent_CloudEventAttributeValue_CeTimestamp{
					CeTimestamp: timestamppb.New(t),
				}
			default:
				responseCode = http.StatusBadRequest
				return nil, v2.NewHTTPResult(http.StatusBadRequest, "invalid delivery time")
			}
			tID, err := cp.ctrl.EventbusService().GetSystemEventbusByName(ctx, primitive.TimerEventbusName)
			if err != nil {
				return nil, err
			}
			req.EventbusId = tID.Id
		}
	}

	eids, err := cp.writeEvents(ctx, vanus.NewIDFromUint64(req.EventbusId), req.Events)
	if err != nil {
		return nil, err
	}
	return &proxypb.PublishResponse{
		EventIds: eids,
	}, nil
}

func (cp *ControllerProxy) writeEvents(
	ctx context.Context, eventbusID vanus.ID, events *cloudevents.CloudEventBatch,
) ([]string, error) {
	val, exist := cp.writerMap.Load(eventbusID)
	if !exist {
		val, _ = cp.writerMap.LoadOrStore(eventbusID,
			cp.client.Eventbus(ctx, api.WithID(eventbusID.Uint64())).Writer())
	}
	w, _ := val.(api.BusWriter)
	eids, err := w.Append(ctx, events)
	if err != nil {
		log.Warning(ctx, "append to failed", map[string]interface{}{
			log.KeyError: err,
			"eventbus":   eventbusID.Key(),
		})
		return nil, v2.NewHTTPResult(http.StatusInternalServerError, err.Error())
	}
	return eids, nil
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

	// TODO add config
	newSink := fmt.Sprintf("http://%s:%d%s/%s",
		os.Getenv("POD_IP"), cp.cfg.SinkPort, httpRequestPrefix, req.SubscriptionId)
	if meta.Sink != newSink {
		if err = cp.disableSubscription(_ctx, req, subscriptionID.Uint64()); err != nil {
			log.Error(_ctx, "disable subscription failed", map[string]interface{}{
				log.KeyError: err,
				"id":         req.SubscriptionId,
			})
			return err
		}

		_, err = cp.triggerCtrl.UpdateSubscription(_ctx,
			newSubscription(meta, subscriptionID.Uint64(), newSink))
		if err != nil {
			log.Error(_ctx, "update subscription sink failed", map[string]interface{}{
				log.KeyError: err,
				"id":         req.SubscriptionId,
			})
			return err
		}

		resumeSubscriptionReq := &ctrlpb.ResumeSubscriptionRequest{
			Id: subscriptionID.Uint64(),
		}
		_, err = cp.triggerCtrl.ResumeSubscription(context.Background(), resumeSubscriptionReq)
		if err != nil {
			log.Error(_ctx, "resume subscription failed", map[string]interface{}{
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
				"eventpb": eventpb.String(),
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

func (cp *ControllerProxy) disableSubscription(
	ctx context.Context, req *proxypb.SubscribeRequest, subscriptionID uint64,
) error {
	disableSubscriptionReq := &ctrlpb.DisableSubscriptionRequest{
		Id:            subscriptionID,
		Declaratively: true,
	}
	_, err := cp.triggerCtrl.DisableSubscription(context.Background(), disableSubscriptionReq)
	if err != nil {
		log.Error(ctx, "disable subscription failed", map[string]interface{}{
			log.KeyError: err,
			"id":         req.SubscriptionId,
		})
		return err
	}

	// TODO(jiangkai): delete me after disable supports synchronization interface
	retryTime := 3
	disable := false
	for i := 0; i < retryTime; i++ {
		s, _ := cp.triggerCtrl.GetSubscription(context.Background(), &ctrlpb.GetSubscriptionRequest{
			Id: subscriptionID,
		})
		if s.Disable {
			disable = true
			break
		}
		stdtime.Sleep(stdtime.Second)
	}
	if !disable {
		return stderr.New("disable is not completed, please try again")
	}
	return nil
}

func newSubscription(
	info *metapb.Subscription, subscriptionID uint64, newsink string,
) *ctrlpb.UpdateSubscriptionRequest {
	return &ctrlpb.UpdateSubscriptionRequest{
		Id: subscriptionID,
		Subscription: &ctrlpb.SubscriptionRequest{
			Source:           info.Source,
			Types:            info.Types,
			Config:           info.Config,
			Filters:          info.Filters,
			Sink:             newsink,
			SinkCredential:   info.SinkCredential,
			Protocol:         info.Protocol,
			ProtocolSettings: info.ProtocolSettings,
			Transformer:      info.Transformer,
			Name:             info.Name,
			Description:      info.Description,
			Disable:          info.Disable,
			NamespaceId:      info.NamespaceId,
			EventbusId:       info.EventbusId,
		},
	}
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
		nsCtrl:       ctrl.NamespaceService().RawClient(),
	}
}

// SetClient just for test.
func (cp *ControllerProxy) SetClient(client eb.Client) {
	cp.client = client
}

func (cp *ControllerProxy) Start() error {
	if err := cp.ctrl.WaitForControllerReady(false); err != nil {
		panic("error when wait cluster become ready, " + err.Error())
	}
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

	c, err := client.NewHTTP(cehttp.WithListener(sinkListen),
		cehttp.WithRequestDataAtContextMiddleware())
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

	sub, _ := cache.(*subscribeCache)

	sequenceID := atomic.AddUint64(&sub.sequenceID, 1)
	var success bool
	donec := make(chan struct{})
	sub.acks.Store(sequenceID, ackCallback(func(result bool) {
		log.Info(_ctx, "ack callback", map[string]interface{}{
			"result": result,
		})
		success = result
		close(donec)
	}))
	sub.eventc <- message{
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

func (cp *ControllerProxy) LookupOffset(
	ctx context.Context, req *proxypb.LookupOffsetRequest,
) (*proxypb.LookupOffsetResponse, error) {
	elList := make([]api.Eventlog, 0)
	if req.EventlogId > 0 {
		id := vanus.NewIDFromUint64(req.EventlogId)
		l, err := cp.client.Eventbus(ctx, api.WithID(req.EventbusId)).GetLog(ctx, id.Uint64())
		if err != nil {
			return nil, err
		}
		elList = append(elList, l)
	} else {
		ls, err := cp.client.Eventbus(ctx, api.WithID(req.EventbusId)).ListLog(ctx)
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

func (cp *ControllerProxy) GetEvent(
	ctx context.Context, req *proxypb.GetEventRequest,
) (*proxypb.GetEventResponse, error) {
	vid := vanus.NewIDFromUint64(req.EventbusId)
	if vid == vanus.EmptyID() {
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

	ls, err := cp.client.Eventbus(ctx, api.WithID(vid.Uint64())).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	reader := cp.client.Eventbus(ctx, api.WithID(vid.Uint64())).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], offset)),
		option.WithBatchSize(int(num)),
	)
	events, _, _, err := api.Read(ctx, reader)
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

func (cp *ControllerProxy) ValidateSubscription(
	ctx context.Context, req *proxypb.ValidateSubscriptionRequest,
) (*proxypb.ValidateSubscriptionResponse, error) {
	if req.GetEvent() == nil {
		res, err := cp.GetEvent(ctx, &proxypb.GetEventRequest{
			EventbusId: req.EventbusId,
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
		return nil, errors.ErrInvalidRequest.WithMessage(
			"failed to unmarshall event to CloudEvent").Wrap(err)
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
func (cp *ControllerProxy) getByEventID(
	ctx context.Context, req *proxypb.GetEventRequest,
) (*proxypb.GetEventResponse, error) {
	logID, off, err := decodeEventID(req.EventId)
	if err != nil {
		return nil, err
	}

	l, err := cp.client.Eventbus(ctx, api.WithID(req.GetEventbusId())).GetLog(ctx, logID)
	if err != nil {
		return nil, err
	}

	reader := cp.client.Eventbus(ctx, api.WithID(req.GetEventbusId())).Reader(
		option.WithReadPolicy(policy.NewManuallyReadPolicy(l, off)),
		option.WithDisablePolling(),
	)
	events, _, _, err := api.Read(ctx, reader)
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
