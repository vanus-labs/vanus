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
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
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
	ResetOffsetToTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error)
}

type trigger struct {
	subscriptionIDStr string

	subscription  *primitive.Subscription
	offsetManager *offset.SubscriptionOffset
	reader        reader.Reader
	eventCh       chan info.EventRecord
	sendCh        chan info.EventRecord
	client        client.EventClient
	filter        filter.Filter
	transformer   *transform.Transformer
	rateLimiter   ratelimit.Limiter
	config        Config

	retryEventCh     chan info.EventRecord
	retryEventReader reader.Reader
	timerEventWriter eventbus.BusWriter
	dlEventWriter    eventbus.BusWriter

	state State
	stop  context.CancelFunc
	lock  sync.RWMutex
	wg    util.Group
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
	return t.client
}

func (t *trigger) changeTarget(sink primitive.URI,
	protocol primitive.Protocol,
	credential primitive.SinkCredential) error {
	eventCli := newEventClient(sink, protocol, credential)
	t.lock.Lock()
	defer t.lock.Unlock()
	t.client = eventCli
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
	if config.MaxRetryAttempts != t.subscription.Config.MaxRetryAttempts {
		t.applyOptions(WithMaxRetryAttempts(config.MaxRetryAttempts))
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

func (t *trigger) sendEvent(ctx context.Context, e *ce.Event) (int, error) {
	var err error
	transformer := t.getTransformer()
	sendEvent := *e
	if transformer != nil {
		// transform will chang event which lost origin event
		sendEvent = e.Clone()
		startTime := time.Now()
		err = transformer.Execute(&sendEvent)
		metrics.TriggerTransformCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
		if err != nil {
			return -1, err
		}
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, t.getConfig().DeliveryTimeout)
	defer cancel()
	t.rateLimiter.Take()
	startTime := time.Now()
	r := t.getClient().Send(timeoutCtx, sendEvent)
	if r == client.Success {
		metrics.TriggerPushEventTime.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
	}
	return r.StatusCode, r.Err
}

func (t *trigger) runRetryEventFilter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.retryEventCh:
			if !ok {
				return
			}
			t.offsetManager.EventReceive(event.OffsetInfo)
			ec, _ := event.Event.Context.(*ce.EventContextV1)
			if len(ec.Extensions) == 0 {
				t.offsetManager.EventCommit(event.OffsetInfo)
				continue
			}
			v, exist := ec.Extensions[primitive.XVanusSubscriptionID]
			if !exist || t.subscriptionIDStr != v.(string) {
				t.offsetManager.EventCommit(event.OffsetInfo)
				continue
			}
			startTime := time.Now()
			res := filter.Run(t.getFilter(), *event.Event)
			metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
			if res == filter.FailFilter {
				t.offsetManager.EventCommit(event.OffsetInfo)
				continue
			}
			t.sendCh <- event
			metrics.TriggerFilterMatchRetryEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
		}
	}
}

func (t *trigger) runEventFilter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.eventCh:
			if !ok {
				return
			}
			t.offsetManager.EventReceive(event.OffsetInfo)
			startTime := time.Now()
			res := filter.Run(t.getFilter(), *event.Event)
			metrics.TriggerFilterCostSecond.WithLabelValues(t.subscriptionIDStr).Observe(time.Since(startTime).Seconds())
			if res == filter.FailFilter {
				t.offsetManager.EventCommit(event.OffsetInfo)
				continue
			}
			t.sendCh <- event
			metrics.TriggerFilterMatchEventCounter.WithLabelValues(t.subscriptionIDStr).Inc()
		}
	}
}

func (t *trigger) runEventSend(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.sendCh:
			if !ok {
				return
			}
			go func(event info.EventRecord) {
				code, err := t.sendEvent(ctx, event.Event)
				if err != nil {
					metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelValuePushEventFail).Inc()
					log.Info(ctx, "send event fail", map[string]interface{}{
						log.KeyError: err,
						"event":      event.Event,
					})
					t.writeFailEvent(ctx, event.Event, code, err)
				} else {
					metrics.TriggerPushEventCounter.WithLabelValues(t.subscriptionIDStr, metrics.LabelValuePushEventSuccess).Inc()
					log.Debug(ctx, "send event success", map[string]interface{}{
						"event": event.Event,
					})
				}
				t.offsetManager.EventCommit(event.OffsetInfo)
			}(event)
		}
	}
}

func (t *trigger) writeFailEvent(ctx context.Context, e *ce.Event, code int, sendErr error) {
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
		t.writeEventToDeadLetter(ctx, e, reason, sendErr.Error())
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
	for {
		startTime := time.Now()
		_, err := t.timerEventWriter.Append(ctx, e)
		metrics.TriggerRetryEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx, "write retry event error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: t.subscription.ID,
				"event":               e,
			})
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
	for {
		startTime := time.Now()
		_, err := t.dlEventWriter.Append(ctx, e)
		metrics.TriggerDeadLetterEventAppendSecond.WithLabelValues(t.subscriptionIDStr).
			Observe(time.Since(startTime).Seconds())
		if err != nil {
			log.Info(ctx, "write dl event error", map[string]interface{}{
				log.KeyError:          err,
				log.KeySubscriptionID: t.subscription.ID,
				"event":               e,
			})
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
	controllers := t.config.Controllers
	sub := t.subscription
	ebVrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s",
		controllers[0],
		sub.EventBus,
		strings.Join(controllers, ","))
	var offsetTimestamp int64
	if sub.Config.OffsetTimestamp != nil {
		offsetTimestamp = int64(*sub.Config.OffsetTimestamp)
	}
	return reader.Config{
		EventBusName:    sub.EventBus,
		EventBusVRN:     ebVrn,
		SubscriptionID:  sub.ID,
		OffsetType:      sub.Config.OffsetType,
		OffsetTimestamp: offsetTimestamp,
		Offset:          getOffset(t.offsetManager, sub),
	}
}

func (t *trigger) getRetryEventReaderConfig() reader.Config {
	controllers := t.config.Controllers
	sub := t.subscription
	ebName := primitive.RetryEventbusName
	ebVrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s",
		controllers[0],
		ebName,
		strings.Join(controllers, ","))
	return reader.Config{
		EventBusName:   ebName,
		EventBusVRN:    ebVrn,
		SubscriptionID: sub.ID,
		OffsetType:     primitive.LatestOffset,
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
	t.client = newEventClient(t.subscription.Sink, t.subscription.Protocol, t.subscription.SinkCredential)
	timerEventWriter, err := newEventbusWriter(ctx, primitive.TimerEventbusName, t.config.Controllers)
	if err != nil {
		return err
	}
	t.timerEventWriter = timerEventWriter
	dlEventWriter, err := newEventbusWriter(ctx, t.config.DeadLetterEventbus, t.config.Controllers)
	if err != nil {
		return err
	}
	t.dlEventWriter = dlEventWriter
	t.eventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.sendCh = make(chan info.EventRecord, t.config.BufferSize)
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
		t.wg.StartWithContext(ctx, t.runEventFilter)
	}
	t.wg.StartWithContext(ctx, t.runEventSend)
	// retry event
	_ = t.retryEventReader.Start()
	t.wg.StartWithContext(ctx, t.runRetryEventFilter)
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
	t.timerEventWriter.Close(ctx)
	t.dlEventWriter.Close(ctx)
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

func (t *trigger) ResetOffsetToTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error) {
	offsets, err := t.reader.GetOffsetByTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}
	t.subscription.Offsets = offsets
	t.offsetManager.Clear()
	return offsets, nil
}

// GetOffsets contains retry eventlog.
func (t *trigger) GetOffsets(ctx context.Context) pInfo.ListOffsetInfo {
	return t.offsetManager.GetCommit()
}
