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

//go:generate mockgen -source=trigger.go -destination=mock_trigger.go -package=trigger
package trigger

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/ratelimit"

	eb "github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util"

	"github.com/vanus-labs/vanus/internal/primitive"
	pInfo "github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/trigger/client"
	"github.com/vanus-labs/vanus/internal/trigger/filter"
	"github.com/vanus-labs/vanus/internal/trigger/info"
	"github.com/vanus-labs/vanus/internal/trigger/offset"
	"github.com/vanus-labs/vanus/internal/trigger/reader"
	"github.com/vanus-labs/vanus/internal/trigger/transform"
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
	batchSendCh   chan []*toSendEvent
	eventCli      client.EventClient
	client        api.Client
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

	pool *ants.Pool
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
	t.offsetManager = offset.NewSubscriptionOffset(subscription.ID, t.config.MaxUACKNumber, subscription.Offsets)
	t.pool, _ = ants.NewPool(t.config.GoroutineSize)
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
	credential primitive.SinkCredential,
) error {
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

func (t *trigger) sendEvent(ctx context.Context, events ...*ce.Event) client.Result {
	timeoutCtx, cancel := context.WithTimeout(ctx, t.getConfig().DeliveryTimeout)
	defer cancel()
	t.rateLimiter.Take()
	startTime := time.Now()
	r := t.getClient().Send(timeoutCtx, events...)
	if r == client.Success {
		metrics.TriggerPushEventTime.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
	}
	return r
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
			_ = t.pool.Submit(func() {
				ec, _ := record.Event.Context.(*ce.EventContextV1)
				if len(ec.Extensions) == 0 {
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				v, exist := ec.Extensions[primitive.XVanusSubscriptionID]
				if !exist || t.subscriptionIDStr != v.(string) {
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				startTime := time.Now()
				res := filter.Run(t.getFilter(), *record.Event)
				metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
				if res == filter.FailFilter {
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				metrics.TriggerFilterMatchRetryEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
				event, err := t.transformEvent(record)
				if err != nil {
					log.Info(ctx).Err(err).
						Str("event_id", event.record.Event.ID()).
						Interface("event_offset", event.record.OffsetInfo).
						Msg("event transform error")
					t.writeFailEvent(ctx, record.Event, ErrTransformCode, err)
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				t.sendCh <- event
			})
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
			_ = t.pool.Submit(func() {
				startTime := time.Now()
				res := filter.Run(t.getFilter(), *record.Event)
				metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
				if res == filter.FailFilter {
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				metrics.TriggerFilterMatchEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
				event, err := t.transformEvent(record)
				if err != nil {
					log.Info(ctx).Err(err).
						Str("event_id", event.record.Event.ID()).
						Interface("event_offset", event.record.OffsetInfo).
						Msg("event transform error")
					t.writeFailEvent(ctx, record.Event, ErrTransformCode, err)
					t.offsetManager.EventCommit(record.OffsetInfo)
					return
				}
				t.sendCh <- event
			})
		}
	}
}

func (t *trigger) runEventToBatch(ctx context.Context) {
	var events []*toSendEvent
	ticker := time.NewTicker(500 * time.Millisecond) ////nolint:gomnd
	defer ticker.Stop()
	var lock sync.Mutex
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lock.Lock()
			if len(events) != 0 {
				e := make([]*toSendEvent, len(events))
				copy(e, events)
				t.batchSendCh <- e
				events = nil
			}
			lock.Unlock()
		case event, ok := <-t.sendCh:
			if !ok {
				return
			}
			lock.Lock()
			events = append(events, event)
			if !t.batch || len(events) >= t.config.SendBatchSize {
				e := make([]*toSendEvent, len(events))
				copy(e, events)
				t.batchSendCh <- e
				events = nil
			}
			lock.Unlock()
		}
	}
}

func (t *trigger) runEventSend(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case events, ok := <-t.batchSendCh:
			if !ok {
				return
			}
			if t.config.Ordered {
				t.processEvent(ctx, events...)
			} else {
				_ = t.pool.Submit(func() {
					t.processEvent(ctx, events...)
				})
			}
		}
	}
}

func (t *trigger) processEvent(ctx context.Context, events ...*toSendEvent) {
	defer func() {
		// commit offset
		for _, event := range events {
			t.offsetManager.EventCommit(event.record.OffsetInfo)
		}
	}()
	es := make([]*ce.Event, len(events))
	for i := range events {
		es[i] = events[i].transform
	}
	r := t.sendEvent(ctx, es...)
	if r != client.Success {
		metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelFailed).
			Add(float64(len(es)))
		log.Info(ctx).Err(r.Err).
			Int("code", r.StatusCode).
			Int("count", len(es)).
			Str("event_id", events[0].record.Event.ID()).
			Interface("event_offset", events[0].record.OffsetInfo).
			Msg("send event fail")
		code := r.StatusCode
		if t.config.Ordered {
			// todo retry util success, now ordered event no need retry direct into dead letter
			code = OrderEventCode
		}
		for _, event := range events {
			t.writeFailEvent(ctx, event.record.Event, code, r.Err)
		}
	} else {
		metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelSuccess).
			Add(float64(len(es)))
		eByte, _ := json.Marshal(es[0])
		log.Debug(ctx).
			Int("code", r.StatusCode).
			Int("count", len(es)).
			Str("event_id", events[0].record.Event.ID()).
			Bytes("event", eByte).
			Msg("send event success")
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
				log.Info(ctx).Err(err).Msg("get retry attempts error")
			}
			if attempts >= t.getConfig().MaxRetryAttempts {
				needRetry = false
				reason = "MaxDeliveryAttemptExceeded"
			}
		}
	}
	if !needRetry {
		// dead letter
		if t.dlEventWriter == nil {
			return
		}
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
	ec.Extensions[primitive.XVanusDeliveryTime] = ce.Timestamp{Time: time.Now().Add(delayTime)}
	ec.Extensions[primitive.XVanusSubscriptionID] = t.subscriptionIDStr
	ec.Extensions[primitive.XVanusEventbus] = t.subscription.RetryEventbusID.Key()
	var writeAttempt int
	for {
		writeAttempt++
		startTime := time.Now()
		_, err := api.AppendOne(ctx, t.timerEventWriter, e)
		metrics.TriggerRetryEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx).Err(err).
				Stringer(log.KeySubscriptionID, t.subscription.ID).
				Int("attempt", writeAttempt).
				Interface("event", e).
				Msg("write retry event error")

			if writeAttempt >= t.config.MaxWriteAttempt {
				return
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	log.Debug(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Interface("event", e).
		Msg("write retry event success")
}

func (t *trigger) writeEventToDeadLetter(ctx context.Context, e *ce.Event, reason, errorMsg string) {
	ec, _ := e.Context.(*ce.EventContextV1)
	delete(ec.Extensions, primitive.XVanusEventbus)
	ec.Extensions[primitive.XVanusSubscriptionID] = t.subscriptionIDStr
	ec.Extensions[primitive.LastDeliveryTime] = ce.Timestamp{Time: time.Now()}
	ec.Extensions[primitive.LastDeliveryError] = errorMsg
	ec.Extensions[primitive.DeadLetterReason] = reason
	var writeAttempt int
	for {
		writeAttempt++
		startTime := time.Now()
		_, err := api.AppendOne(ctx, t.dlEventWriter, e)
		metrics.TriggerDeadLetterEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx).Err(err).
				Stringer(log.KeySubscriptionID, t.subscription.ID).
				Int("attempt", writeAttempt).
				Interface("event", e).
				Msg("write dl event error")
			if writeAttempt >= t.config.MaxWriteAttempt {
				return
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	log.Debug(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Interface("event", e).
		Msg("write dl event success")
}

func (t *trigger) getReaderConfig() reader.Config {
	return reader.Config{
		EventbusID:     t.subscription.EventbusID,
		Client:         t.client,
		SubscriptionID: t.subscription.ID,
		BatchSize:      t.config.PullBatchSize,
		Offset:         getOffset(t.subscription),
	}
}

func (t *trigger) getRetryEventReaderConfig() reader.Config {
	return reader.Config{
		EventbusID:     t.subscription.RetryEventbusID,
		Client:         t.client,
		SubscriptionID: t.subscription.ID,
		BatchSize:      t.config.PullBatchSize,
		Offset:         getOffset(t.subscription),
	}
}

// getOffset from subscription.
func getOffset(sub *primitive.Subscription) map[vanus.ID]uint64 {
	// get offset from subscription
	offsetMap := make(map[vanus.ID]uint64)
	for _, o := range sub.Offsets {
		offsetMap[o.EventlogID] = o.Offset
	}
	return offsetMap
}

func (t *trigger) Init(ctx context.Context) error {
	t.eventCli = newEventClient(t.subscription.Sink, t.subscription.Protocol, t.subscription.SinkCredential)
	t.client = eb.Connect(t.config.Controllers)

	eb, err := t.client.Eventbus(ctx, api.WithID(t.subscription.TimerEventbusID.Uint64()))
	if err != nil {
		return err
	}
	t.timerEventWriter = eb.Writer()
	if !t.config.DisableDeadLetter {
		eb, err = t.client.Eventbus(ctx, api.WithID(t.subscription.DeadLetterEventbusID.Uint64()))
		if err != nil {
			return err
		}
		t.dlEventWriter = eb.Writer()
	}
	t.eventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.sendCh = make(chan *toSendEvent, t.config.BufferSize)
	t.batchSendCh = make(chan []*toSendEvent, t.config.BufferSize)
	t.reader = reader.NewReader(t.getReaderConfig(), t.eventCh)
	t.retryEventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.retryEventReader = reader.NewReader(t.getRetryEventReaderConfig(), t.retryEventCh)
	return nil
}

func (t *trigger) Start(ctx context.Context) error {
	log.Info(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Msg("trigger start...")
	ctx, cancel := context.WithCancel(context.Background())
	t.stop = cancel
	// eb event
	err := t.reader.Start()
	if err != nil {
		return err
	}
	// retry event
	err = t.retryEventReader.Start()
	if err != nil {
		t.reader.Close()
		return err
	}
	t.wg.StartWithContext(ctx, t.runEventFilterTransform)
	t.wg.StartWithContext(ctx, t.runEventToBatch)
	t.wg.StartWithContext(ctx, t.runEventSend)
	t.wg.StartWithContext(ctx, t.runRetryEventFilterTransform)
	t.state = TriggerRunning
	log.Info(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Msg("trigger started")
	return nil
}

func (t *trigger) Stop(ctx context.Context) error {
	log.Info(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Msg("trigger stop...")

	if t.state == TriggerStopped {
		return nil
	}
	t.reader.Close()
	t.retryEventReader.Close()
	t.stop()
	close(t.eventCh)
	close(t.retryEventCh)
	close(t.sendCh)
	close(t.batchSendCh)
	t.wg.Wait()
	t.pool.Release()
	t.offsetManager.Close()
	t.state = TriggerStopped
	log.Info(ctx).
		Stringer(log.KeySubscriptionID, t.subscription.ID).
		Msg("trigger stopped")
	return nil
}

func (t *trigger) Change(_ context.Context, subscription *primitive.Subscription) error {
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
func (t *trigger) GetOffsets(_ context.Context) pInfo.ListOffsetInfo {
	return t.offsetManager.GetCommit()
}
