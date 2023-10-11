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
	pInfo "github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/trigger/info"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/pkg/util"
)

const (
	lookupReadableLogsTimeout = 5 * time.Second
	readEventTimeout          = 5 * time.Second
	readErrSleepTime          = 2 * time.Second
)

type Config struct {
	EventbusID        vanus.ID
	Client            eb.Client
	SubscriptionID    vanus.ID
	SubscriptionIDStr string
	EventbusIDStr     string
	Offset            EventlogOffset
	BatchSize         int
}
type EventlogOffset map[vanus.ID]uint64

type Reader interface {
	Start() error
	Close()
}

type reader struct {
	config      Config
	events      chan<- info.EventRecord
	stop        context.CancelFunc
	wg          sync.WaitGroup
	eventlogMap map[uint64]*eventlogReader
}

func NewReader(config Config, events chan<- info.EventRecord) Reader {
	config.SubscriptionIDStr = config.SubscriptionID.Key()
	config.EventbusIDStr = config.EventbusID.Key()
	r := &reader{
		config:      config,
		events:      events,
		eventlogMap: map[uint64]*eventlogReader{},
	}
	return r
}

func (r *reader) Close() {
	if r.stop != nil {
		r.stop()
	}
	r.wg.Wait()
	log.Info().
		Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
		Str(log.KeyEventbusID, r.config.EventbusIDStr).
		Msg("reader closed")
}

func (r *reader) findEventlog(ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, lookupReadableLogsTimeout)
	defer cancel()
	logs, err := r.config.Client.Eventbus(timeoutCtx, api.WithID(r.config.EventbusID.Uint64())).ListLog(timeoutCtx)
	if err != nil {
		log.Warn().Err(err).
			Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
			Str(log.KeyEventbusID, r.config.EventbusIDStr).
			Msg("eventbus lookup Readable eventlog error")
		return err
	}
	logsMap := make(map[uint64]api.Eventlog, len(logs))
	for i := range logs {
		logsMap[logs[i].ID()] = logs[i]
	}
	for id, l := range logsMap {
		if _, exist := r.eventlogMap[id]; exist {
			continue
		}
		eventlogID := vanus.NewIDFromUint64(id)
		log.Info().
			Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
			Str(log.KeyEventbusID, r.config.EventbusIDStr).
			Str(log.KeyEventlogID, eventlogID.Key()).
			Msg("find new eventlog will start log reader")
		r.startEventlog(ctx, l)
	}
	for id, el := range r.eventlogMap {
		if _, exist := logsMap[id]; exist {
			continue
		}
		log.Info().
			Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
			Str(log.KeyEventbusID, r.config.EventbusIDStr).
			Str(log.KeyEventlogID, vanus.NewIDFromUint64(id).Key()).
			Msg("find no exist eventlog will stop log reader")
		el.stop()
	}
	return nil
}

func (r *reader) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.stop = cancel
	err := r.findEventlog(ctx)
	if err != nil {
		return err
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(time.Minute * 5)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.findEventlog(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (r *reader) startEventlog(ctx context.Context, l api.Eventlog) {
	eventlogID := vanus.NewIDFromUint64(l.ID())
	offset := r.getOffset(eventlogID)
	elc := &eventlogReader{
		config:        r.config,
		eventlogID:    eventlogID,
		eventlogIDStr: eventlogID.Key(),
		policy:        policy.NewManuallyReadPolicy(l, int64(offset)),
		events:        r.events,
		offset:        offset,
	}
	r.eventlogMap[l.ID()] = elc
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		log.Info().
			Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
			Str(log.KeyEventbusID, r.config.EventbusIDStr).
			Str(log.KeyEventlogID, elc.eventlogIDStr).
			Uint64("offset", elc.offset).
			Msg("event eventlog reader start")
		elc.run(ctx)
		log.Info().
			Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
			Str(log.KeyEventbusID, r.config.EventbusIDStr).
			Str(log.KeyEventlogID, elc.eventlogIDStr).
			Uint64("offset", elc.offset).
			Msg("event eventlog reader stop")
	}()
}

func (r *reader) getOffset(eventlogID vanus.ID) uint64 {
	v, exist := r.config.Offset[eventlogID]
	if exist {
		return v
	}
	log.Warn().
		Str(log.KeySubscriptionID, r.config.SubscriptionIDStr).
		Str(log.KeyEventbusID, r.config.EventbusIDStr).
		Str(log.KeyEventlogID, eventlogID.Key()).
		Msg("offset no exist, will use 0")
	return 0
}

type eventlogReader struct {
	config        Config
	eventlogID    vanus.ID
	eventlogIDStr string
	policy        api.ReadPolicy
	events        chan<- info.EventRecord
	offset        uint64
	cancel        context.CancelFunc
}

func (elReader *eventlogReader) stop() {
	if elReader.cancel == nil {
		return
	}
	elReader.cancel()
}

func (elReader *eventlogReader) run(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	elReader.cancel = cancel
	r := elReader.config.Client.Eventbus(ctx, api.WithID(elReader.config.EventbusID.Uint64())).Reader(
		option.WithReadPolicy(elReader.policy), option.WithBatchSize(elReader.config.BatchSize))
	log.Info().
		Str(log.KeySubscriptionID, elReader.config.SubscriptionIDStr).
		Str(log.KeyEventbusID, elReader.config.EventbusIDStr).
		Str(log.KeyEventlogID, elReader.eventlogIDStr).
		Interface("offset", elReader.offset).
		Msg("eventlog reader init success")

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
			log.Info().Err(err).
				Str(log.KeySubscriptionID, elReader.config.SubscriptionIDStr).
				Str(log.KeyEventbusID, elReader.config.EventbusIDStr).
				Str(log.KeyEventlogID, elReader.eventlogIDStr).
				Interface("offset", elReader.offset).
				Msg("read event")
		}
		switch {
		case err == nil, errors.Is(err, errors.ErrOffsetOnEnd), errors.Is(err, errors.ErrTryAgain):
			continue
		case stderr.Is(err, context.Canceled), status.Convert(err).Code() == codes.Canceled:
			return
		case errors.Is(err, errors.ErrOffsetUnderflow):
		// todo reset offset timestamp
		default:
			log.Warn().Err(err).
				Str(log.KeySubscriptionID, elReader.config.SubscriptionIDStr).
				Str(log.KeyEventbusID, elReader.config.EventbusIDStr).
				Str(log.KeyEventlogID, elReader.eventlogIDStr).
				Interface("offset", elReader.offset).Msg("read event error")
			if !util.SleepWithContext(ctx, readErrSleepTime) {
				return
			}
		}
	}
}

func (elReader *eventlogReader) loop(ctx context.Context, lr api.BusReader) error {
	events, err := readEvents(ctx, lr)
	if err != nil {
		return err
	}
	for i := range events {
		ec, _ := events[i].Context.(*ce.EventContextV1)
		offsetByte, _ := ec.Extensions[eventlog.XVanusLogOffset].([]byte)
		offset := binary.BigEndian.Uint64(offsetByte)
		eo := info.EventRecord{Event: events[i], OffsetInfo: pInfo.OffsetInfo{
			EventlogID: elReader.eventlogID,
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
		elReader.config.SubscriptionIDStr, elReader.config.EventbusID.Key(), elReader.eventlogIDStr).
		Add(float64(len(events)))
	return nil
}

func (elReader *eventlogReader) putEvent(ctx context.Context, event info.EventRecord) error {
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
