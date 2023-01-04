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

//go:generate mockgen -source=trigger.go  -destination=mock_trigger.go -package=trigger
package trigger

import (
	"context"
	"reflect"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/client"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	"github.com/linkall-labs/vanus/internal/trigger/transform"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/pkg/util"
	"go.uber.org/ratelimit"
)

type State string

const (
	TriggerCreated   = "created"
	TriggerPending   = "pending"
	TriggerRunning   = "running"
	TriggerSleep     = "sleep"
	TriggerPaused    = "paused"
	TriggerStopped   = "stopped"
	TriggerDestroyed = "destroyed"
)

type Trigger interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Change(ctx context.Context, subscription *primitive.Subscription) error
	GetOffsets(ctx context.Context) pInfo.ListOffsetInfo
}

type trigger struct {
	subscriptionIDStr string

	subscription  *primitive.Subscription
	offsetManager *offset.SubscriptionOffset
	reader        reader.Reader
	eventCh       chan info.EventRecord
	sendCh        chan *toSendEvent
	eventCli      client.EventClient
	client        eb.Client
	filter        filter.Filter
	transformer   *transform.Transformer
	rateLimiter   ratelimit.Limiter
	config        Config
	batch         bool

	retryEventCh     chan info.EventRecord
	retryEventReader reader.Reader
	timerEventWriter api.BusWriter
	dlEventWriter    api.BusWriter

	state State
	stop  context.CancelFunc
	lock  sync.RWMutex
	wg    util.Group
}

type toSendEvent struct {
	record    info.EventRecord
	transform *ce.Event
}

func NewTrigger(subscription *primitive.Subscription, opts ...Option) Trigger {
	t := &trigger{
		stop:              func() {},
		config:            defaultConfig(),
		state:             TriggerCreated,
		filter:            filter.GetFilter(subscription.Filters),
		offsetManager:     offset.NewSubscriptionOffset(subscription.ID),
		subscription:      subscription,
		subscriptionIDStr: subscription.ID.String(),
		transformer:       transform.NewTransformer(subscription.Transformer),
	}
	if subscription.Protocol == primitive.GRPC {
		t.batch = true
	}
	t.applyOptions(opts...)
	if t.rateLimiter == nil {
		t.rateLimiter = ratelimit.NewUnlimited()
	}
	return t
}

func (t *trigger) applyOptions(opts ...Option) {
	for _, fn := range opts {
		fn(t)
	}
}

func (t *trigger) getConfig() Config {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.config
}

func (t *trigger) getClient() client.EventClient {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.eventCli
}

func (t *trigger) changeTarget(sink primitive.URI,
	protocol primitive.Protocol,
	credential primitive.SinkCredential) error {
	eventCli := newEventClient(sink, protocol, credential)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.eventCli = eventCli
	t.subscription.Sink = sink
	t.subscription.Protocol = protocol
	t.subscription.SinkCredential = credential
	return nil
}

func (t *trigger) getFilter() filter.Filter {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.filter
}

func (t *trigger) changeFilter(filters []*primitive.SubscriptionFilter) {
	f := filter.GetFilter(filters)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.filter = f
	t.subscription.Filters = filters
}

func (t *trigger) getTransformer() *transform.Transformer {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.transformer
}

func (t *trigger) changeTransformer(transformer *primitive.Transformer) {
	trans := transform.NewTransformer(transformer)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.transformer = trans
	t.subscription.Transformer = transformer
}

func (t *trigger) changeConfig(config primitive.SubscriptionConfig) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if config.RateLimit != t.subscription.Config.RateLimit {
		t.applyOptions(WithRateLimit(config.RateLimit))
	}
	if config.DeliveryTimeout != t.subscription.Config.DeliveryTimeout {
		t.applyOptions(WithDeliveryTimeout(config.DeliveryTimeout))
	}
	if config.GetMaxRetryAttempts() != t.subscription.Config.GetMaxRetryAttempts() {
		t.applyOptions(WithMaxRetryAttempts(config.GetMaxRetryAttempts()))
	}
	t.subscription.Config = config
}

// eventArrived for test.
func (t *trigger) eventArrived(ctx context.Context, event info.EventRecord) error {
	select {
	case t.eventCh <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *trigger) transformEvent(record info.EventRecord) (*toSendEvent, error) {
	transformer := t.getTransformer()
	event := record.Event
	if transformer != nil {
		// transform will chang event which lost origin event
		clone := record.Event.Clone()
		event = &clone
		startTime := time.Now()
		err := transformer.Execute(event)
		metrics.TriggerTransformCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return nil, err
		}
	}
	return &toSendEvent{record: record, transform: event}, nil
}

func (t *trigger) sendEvent(ctx context.Context, events ...*ce.Event) (int, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, t.getConfig().DeliveryTimeout)
	defer cancel()
	t.rateLimiter.Take()
	startTime := time.Now()
	r := t.getClient().Send(timeoutCtx, events...)
	if r == client.Success {
		metrics.TriggerPushEventTime.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
	}
	return r.StatusCode, r.Err
}

func (t *trigger) runRetryEventFilterTransform(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-t.retryEventCh:
			if !ok {
				return
			}
			t.offsetManager.EventReceive(record.OffsetInfo)
			ec, _ := record.Event.Context.(*ce.EventContextV1)
			if len(ec.Extensions) == 0 {
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			v, exist := ec.Extensions[primitive.XVanusSubscriptionID]
			if !exist || t.subscriptionIDStr != v.(string) {
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			startTime := time.Now()
			res := filter.Run(t.getFilter(), *record.Event)
			metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
			if res == filter.FailFilter {
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			metrics.TriggerFilterMatchRetryEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
			event, err := t.transformEvent(record)
			if err != nil {
				t.writeFailEvent(ctx, record.Event, ErrTransformCode, err)
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			t.sendCh <- event
		}
	}
}

func (t *trigger) runEventFilterTransform(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-t.eventCh:
			if !ok {
				return
			}
			t.offsetManager.EventReceive(record.OffsetInfo)
			startTime := time.Now()
			res := filter.Run(t.getFilter(), *record.Event)
			metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
			if res == filter.FailFilter {
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			metrics.TriggerFilterMatchEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
			event, err := t.transformEvent(record)
			if err != nil {
				t.writeFailEvent(ctx, record.Event, ErrTransformCode, err)
				t.offsetManager.EventCommit(record.OffsetInfo)
				continue
			}
			t.sendCh <- event
		}
	}
}

func (t *trigger) runEventSend(ctx context.Context) {
	var events []*toSendEvent
	lastSend := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.sendCh:
			if !ok {
				return
			}
			if !t.batch {
				if t.config.Ordered {
					t.processEvent(ctx, event)
				} else {
					go func(event *toSendEvent) {
						t.processEvent(ctx, event)
					}(event)
				}
			} else {
				events = append(events, event)
				now := time.Now()
				if len(events) >= 32 || now.Sub(lastSend) > 10*time.Millisecond {
					t.processEvent(ctx, events...)
					events = nil
					lastSend = now
				}
			}
		}
	}
}

func (t *trigger) processEvent(ctx context.Context, events ...*toSendEvent) {
	es := make([]*ce.Event, len(events))
	for i := range events {
		es[i] = events[i].transform
	}
	code, err := t.sendEvent(ctx, es...)
	if err != nil {
		metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelValuePushEventFail).Inc()
		log.Info(ctx, "send event fail", map[string]interface{}{
			log.KeyError: err,
			"event":      events,
		})
		if t.config.Ordered {
			// ordered event no need retry direct into dead letter
			code = OrderEventCode
		}
		for _, event := range events {
			t.writeFailEvent(ctx, event.record.Event, code, err)
		}
	} else {
		metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelValuePushEventSuccess).Inc()
		log.Debug(ctx, "send event success", map[string]interface{}{
			"event": events,
		})
	}
	for _, event := range events {
		t.offsetManager.EventCommit(event.record.OffsetInfo)
	}
}
func (t *trigger) writeFailEvent(ctx context.Context, e *ce.Event, code int, err error) {
	needRetry, reason := isShouldRetry(code)
	ec, _ := e.Context.(*ce.EventContextV1)
	if ec.Extensions == nil {
		ec.Extensions = make(map[string]interface{})
	}
	attempts := int32(0)
	if needRetry {
		// get attempts
		if v, ok := ec.Extensions[primitive.XVanusRetryAttempts]; ok {
			var err error
			attempts, err = getRetryAttempts(v)
			if err != nil {
				log.Info(ctx, "get retry attempts error", map[string]interface{}{
					log.KeyError: err,
				})
			}
			if attempts >= t.getConfig().MaxRetryAttempts {
				needRetry = false
				reason = "MaxDeliveryAttemptExceeded"
			}
		}
	}
	if !needRetry {
		// dead letter
		t.writeEventToDeadLetter(ctx, e, reason, err.Error())
		metrics.TriggerDeadLetterEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
		return
	}
	// retry
	t.writeEventToRetry(ctx, e, attempts)
	metrics.TriggerRetryEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
}

func (t *trigger) writeEventToRetry(ctx context.Context, e *ce.Event, attempts int32) {
	ec, _ := e.Context.(*ce.EventContextV1)
	attempts++
	ec.Extensions[primitive.XVanusRetryAttempts] = attempts
	delayTime := calDeliveryTime(attempts)
	ec.Extensions[primitive.XVanusDeliveryTime] = ce.Timestamp{Time: time.Now().Add(delayTime).UTC()}.Format(time.RFC3339)
	ec.Extensions[primitive.XVanusSubscriptionID] = t.subscriptionIDStr
	ec.Extensions[primitive.XVanusEventbus] = primitive.RetryEventbusName
	var writeAttempt int
	for {
		writeAttempt++
		startTime := time.Now()
		_, err := t.timerEventWriter.AppendOne(ctx, e)
		metrics.TriggerRetryEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx, "write retry event error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: t.subscription.ID,
				"attempt":             writeAttempt,
				"event":               e,
			})
			if writeAttempt >= t.config.MaxWriteAttempt {
				return
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	log.Debug(ctx, "write retry event success", map[string]interface{}{
		log.KeyEventlogID: t.subscription.ID,
		"event":           e,
	})
}

func (t *trigger) writeEventToDeadLetter(ctx context.Context, e *ce.Event, reason, errorMsg string) {
	ec, _ := e.Context.(*ce.EventContextV1)
	delete(ec.Extensions, primitive.XVanusEventbus)
	ec.Extensions[primitive.XVanusSubscriptionID] = t.subscriptionIDStr
	ec.Extensions[primitive.LastDeliveryTime] = ce.Timestamp{Time: time.Now().UTC()}.Format(time.RFC3339)
	ec.Extensions[primitive.LastDeliveryError] = errorMsg
	ec.Extensions[primitive.DeadLetterReason] = reason
	var writeAttempt int
	for {
		writeAttempt++
		startTime := time.Now()
		_, err := t.dlEventWriter.AppendOne(ctx, e)
		metrics.TriggerDeadLetterEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx, "write dl event error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: t.subscription.ID,
				"attempt":             writeAttempt,
				"event":               e,
			})
			if writeAttempt >= t.config.MaxWriteAttempt {
				return
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	log.Debug(ctx, "write dl event success", map[string]interface{}{
		log.KeyEventlogID: t.subscription.ID,
		"event":           e,
	})
}

func (t *trigger) getReaderConfig() reader.Config {
	sub := t.subscription
	return reader.Config{
		EventBusName:   sub.EventBus,
		Client:         t.client,
		SubscriptionID: sub.ID,
		Offset:         getOffset(t.offsetManager, sub),
	}
}

func (t *trigger) getRetryEventReaderConfig() reader.Config {
	sub := t.subscription
	ebName := primitive.RetryEventbusName
	return reader.Config{
		EventBusName:   ebName,
		Client:         t.client,
		SubscriptionID: sub.ID,
		Offset:         getOffset(t.offsetManager, sub),
	}
}

// getOffset from subscription,if subscriptionOffset exist,use subscriptionOffset.
func getOffset(subscriptionOffset *offset.SubscriptionOffset, sub *primitive.Subscription) map[vanus.ID]uint64 {
	// get offset from subscription
	offsetMap := make(map[vanus.ID]uint64)
	for _, o := range sub.Offsets {
		offsetMap[o.EventLogID] = o.Offset
	}
	// get offset from offset manager
	offsets := subscriptionOffset.GetCommit()
	for _, offset := range offsets {
		offsetMap[offset.EventLogID] = offset.Offset
	}
	return offsetMap
}

func (t *trigger) Init(ctx context.Context) error {
	t.eventCli = newEventClient(t.subscription.Sink, t.subscription.Protocol, t.subscription.SinkCredential)
	t.client = eb.Connect(t.config.Controllers)

	t.timerEventWriter = t.client.Eventbus(ctx, primitive.TimerEventbusName).Writer()
	t.dlEventWriter = t.client.Eventbus(ctx, t.config.DeadLetterEventbus).Writer()
	t.eventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.sendCh = make(chan *toSendEvent, t.config.BufferSize)
	t.reader = reader.NewReader(t.getReaderConfig(), t.eventCh)
	t.retryEventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.retryEventReader = reader.NewReader(t.getRetryEventReaderConfig(), t.retryEventCh)
	t.offsetManager.Clear()
	return nil
}

func (t *trigger) Start(ctx context.Context) error {
	log.Info(ctx, "trigger start...", map[string]interface{}{
		log.KeySubscriptionID: t.subscription.ID,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.stop = cancel
	// eb event
	_ = t.reader.Start()
	for i := 0; i < t.config.FilterProcessSize; i++ {
		t.wg.StartWithContext(ctx, t.runEventFilterTransform)
	}
	t.wg.StartWithContext(ctx, t.runEventSend)
	// retry event
	_ = t.retryEventReader.Start()
	t.wg.StartWithContext(ctx, t.runRetryEventFilterTransform)
	t.state = TriggerRunning
	log.Info(ctx, "trigger started", map[string]interface{}{
		log.KeySubscriptionID: t.subscription.ID,
	})
	return nil
}

func (t *trigger) Stop(ctx context.Context) error {
	log.Info(ctx, "trigger stop...", map[string]interface{}{
		log.KeySubscriptionID: t.subscription.ID,
	})
	if t.state == TriggerStopped {
		return nil
	}
	t.stop()
	t.reader.Close()
	t.retryEventReader.Close()
	t.wg.Wait()
	close(t.eventCh)
	close(t.sendCh)
	close(t.retryEventCh)
	t.state = TriggerStopped
	log.Info(ctx, "trigger stopped", map[string]interface{}{
		log.KeySubscriptionID: t.subscription.ID,
	})
	return nil
}

func (t *trigger) Change(ctx context.Context, subscription *primitive.Subscription) error {
	if t.subscription.Sink != subscription.Sink ||
		t.subscription.Protocol != subscription.Protocol ||
		!reflect.DeepEqual(t.subscription.SinkCredential, subscription.SinkCredential) {
		err := t.changeTarget(subscription.Sink, subscription.Protocol, subscription.SinkCredential)
		if err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(t.subscription.Filters, subscription.Filters) {
		t.changeFilter(subscription.Filters) //nolint:contextcheck // wrong advice
	}
	if !reflect.DeepEqual(t.subscription.Transformer, subscription.Transformer) {
		t.changeTransformer(subscription.Transformer)
	}
	if !reflect.DeepEqual(t.subscription.Config, subscription.Config) {
		t.changeConfig(subscription.Config)
	}
	return nil
}

// GetOffsets contains retry eventlog.
func (t *trigger) GetOffsets(ctx context.Context) pInfo.ListOffsetInfo {
	return t.offsetManager.GetCommit()
}
