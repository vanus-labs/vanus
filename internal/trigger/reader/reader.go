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

//go:generate mockgen -source=reader.go  -destination=mock_reader.go -package=reader
package reader

import (
	"context"
	"encoding/binary"
	stderr "errors"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eb "github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventlog"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/pkg/util"

	pInfo "github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/trigger/info"
)

const (
	lookupReadableLogsTimeout = 5 * time.Second
	readEventTimeout          = 5 * time.Second
	readErrSleepTime          = 2 * time.Second
)

type Config struct {
	EventBusName      string
	Client            eb.Client
	SubscriptionID    vanus.ID
	SubscriptionIDStr string
	Offset            EventLogOffset
	BatchSize         int
}
type EventLogOffset map[vanus.ID]uint64

type Reader interface {
	Start() error
	Close()
}

type reader struct {
	config   Config
	elReader map[vanus.ID]struct{}
	events   chan<- info.EventRecord
	stop     context.CancelFunc
	wg       sync.WaitGroup
}

func NewReader(config Config, events chan<- info.EventRecord) Reader {
	config.SubscriptionIDStr = config.SubscriptionID.String()
	r := &reader{
		config:   config,
		events:   events,
		elReader: make(map[vanus.ID]struct{}),
	}
	return r
}

func (r *reader) Close() {
	if r.stop != nil {
		r.stop()
	}
	r.wg.Wait()
	log.Info(context.TODO(), "reader closed", map[string]interface{}{
		log.KeyEventbusName: r.config.EventBusName,
	})
}

func (r *reader) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.stop = cancel
	timeoutCtx, cancel := context.WithTimeout(ctx, lookupReadableLogsTimeout)
	defer cancel()
	logs, err := r.config.Client.Eventbus(timeoutCtx, r.config.EventBusName).ListLog(timeoutCtx)
	if err != nil {
		log.Warning(ctx, "eventbus lookup Readable eventlog error", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBusName,
			log.KeyError:        err,
		})
		return err
	}
	for _, l := range logs {
		eventLogID := vanus.NewIDFromUint64(l.ID())
		offset := r.getOffset(eventLogID)
		elc := &eventLogReader{
			config:        r.config,
			eventLogID:    eventLogID,
			eventLogIDStr: eventLogID.String(),
			policy:        policy.NewManuallyReadPolicy(l, int64(offset)),
			events:        r.events,
			offset:        offset,
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			log.Info(ctx, "event eventlog reader start", map[string]interface{}{
				log.KeyEventbusName: elc.config.EventBusName,
				log.KeyEventlogID:   elc.eventLogID,
				"offset":            elc.offset,
			})
			elc.run(ctx)
			log.Info(ctx, "event eventlog reader stop", map[string]interface{}{
				log.KeyEventbusName: elc.config.EventBusName,
				log.KeyEventlogID:   elc.eventLogID,
				"offset":            elc.offset,
			})
		}()
	}
	return nil
}

func (r *reader) getOffset(eventLogID vanus.ID) uint64 {
	v, exist := r.config.Offset[eventLogID]
	if exist {
		return v
	}
	log.Warning(context.TODO(), "offset no exist, will use 0", map[string]interface{}{
		log.KeyEventbusName:   r.config.EventBusName,
		log.KeySubscriptionID: r.config.SubscriptionID,
		log.KeyEventlogID:     eventLogID,
	})
	return 0
}

type eventLogReader struct {
	config        Config
	eventLogID    vanus.ID
	eventLogIDStr string
	policy        api.ReadPolicy
	events        chan<- info.EventRecord
	offset        uint64
}

func (elReader *eventLogReader) run(ctx context.Context) {
	r := elReader.config.Client.Eventbus(ctx, elReader.config.EventBusName).Reader(
		option.WithReadPolicy(elReader.policy), option.WithBatchSize(elReader.config.BatchSize))
	log.Info(ctx, "eventlog reader init success", map[string]interface{}{
		log.KeyEventbusName: elReader.config.EventBusName,
		log.KeyEventlogID:   elReader.eventLogID,
		"offset":            elReader.offset,
	})
	min := time.Now().Minute()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := elReader.loop(ctx, r)
		if time.Now().Minute() != min {
			min = time.Now().Minute()
			log.Info(ctx, "read event", map[string]interface{}{
				log.KeyEventbusName: elReader.config.EventBusName,
				log.KeyEventlogID:   elReader.eventLogIDStr,
				log.KeyError:        err,
				"offset":            elReader.offset,
			})
		}
		switch {
		case err == nil, errors.Is(err, errors.ErrOffsetOnEnd), errors.Is(err, errors.ErrTryAgain):
			continue
		case stderr.Is(err, context.Canceled), status.Convert(err).Code() == codes.Canceled:
			return
		case errors.Is(err, errors.ErrOffsetUnderflow):
			// todo reset offset timestamp
		default:
			log.Warning(ctx, "read event error", map[string]interface{}{
				log.KeyEventbusName: elReader.config.EventBusName,
				log.KeyEventlogID:   elReader.eventLogID,
				"offset":            elReader.offset,
				log.KeyError:        err,
			})
			if !util.SleepWithContext(ctx, readErrSleepTime) {
				return
			}
		}
	}
}

func (elReader *eventLogReader) loop(ctx context.Context, lr api.BusReader) error {
	events, err := readEvents(ctx, lr)
	if err != nil {
		return err
	}
	for i := range events {
		ec, _ := events[i].Context.(*ce.EventContextV1)
		offsetByte, _ := ec.Extensions[eventlog.XVanusLogOffset].([]byte)
		offset := binary.BigEndian.Uint64(offsetByte)
		eo := info.EventRecord{Event: events[i], OffsetInfo: pInfo.OffsetInfo{
			EventLogID: elReader.eventLogID,
			Offset:     offset,
		}}
		delete(ec.Extensions, eventlog.XVanusLogOffset)
		if err = elReader.putEvent(ctx, eo); err != nil {
			return err
		}
		elReader.offset = offset
	}
	elReader.policy.Forward(len(events))
	metrics.TriggerPullEventCounter.WithLabelValues(
		elReader.config.SubscriptionIDStr, elReader.config.EventBusName, elReader.eventLogIDStr).
		Add(float64(len(events)))
	return nil
}

func (elReader *eventLogReader) putEvent(ctx context.Context, event info.EventRecord) error {
	select {
	case elReader.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func readEvents(ctx context.Context, lr api.BusReader) ([]*ce.Event, error) {
	timeout, cancel := context.WithTimeout(ctx, readEventTimeout)
	defer cancel()
	events, _, _, err := api.Read(timeout, lr)
	return events, err
}
