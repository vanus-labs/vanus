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
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	eberrors "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/option"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/pkg/util"
)

const (
	checkEventLogInterval     = 30 * time.Second
	lookupReadableLogsTimeout = 5 * time.Second
	readerSeekTimeout         = 5 * time.Second
	readEventTimeout          = 5 * time.Second
	initErrSleepTime          = 5 * time.Second
	readErrSleepTime          = 2 * time.Second
	readSize                  = 5
)

type Config struct {
	EventBusName      string
	Controllers       []string
	Client            eb.Client
	SubscriptionID    vanus.ID
	SubscriptionIDStr string
	Offset            EventLogOffset
	OffsetType        primitive.OffsetType
	OffsetTimestamp   int64

	CheckEventLogInterval time.Duration
}
type EventLogOffset map[vanus.ID]uint64

type Reader interface {
	Start() error
	GetOffsetByTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error)
	Close()
}

type reader struct {
	config   Config
	elReader map[vanus.ID]struct{}
	events   chan<- info.EventRecord
	stop     context.CancelFunc
	stctx    context.Context
	wg       sync.WaitGroup
	lock     sync.Mutex
}

func NewReader(config Config, events chan<- info.EventRecord) Reader {
	if config.CheckEventLogInterval <= 0 {
		config.CheckEventLogInterval = checkEventLogInterval
	}
	config.SubscriptionIDStr = config.SubscriptionID.String()
	r := &reader{
		config:   config,
		events:   events,
		elReader: make(map[vanus.ID]struct{}),
	}
	r.stctx, r.stop = context.WithCancel(context.Background())
	return r
}

func (r *reader) GetOffsetByTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error) {
	offsets := make(pInfo.ListOffsetInfo, 0, len(r.elReader))
	bus := r.config.Client.Eventbus(ctx, r.config.EventBusName)
	for id := range r.elReader {
		log, err := bus.GetLog(ctx, id.Uint64())
		if err != nil {
			return offsets, err
		}
		offset, err := log.QueryOffsetByTime(ctx, timestamp)
		if err != nil {
			return offsets, err
		}
		offsets = append(offsets, pInfo.OffsetInfo{
			EventLogID: id,
			Offset:     uint64(offset),
		})
	}
	return offsets, nil
}

func (r *reader) Close() {
	r.stop()
	r.wg.Wait()
	log.Info(r.stctx, "reader closed", map[string]interface{}{
		log.KeyEventbusName: r.config.EventBusName,
	})
}

func (r *reader) Start() error {
	go r.watchEventLogChange()
	return nil
}

func (r *reader) watchEventLogChange() {
	r.checkEventLogChange()
	tk := time.NewTicker(r.config.CheckEventLogInterval)
	defer tk.Stop()
	for {
		select {
		case <-r.stctx.Done():
			return
		case <-tk.C:
			r.checkEventLogChange()
		}
	}
}

func (r *reader) checkEventLogChange() {
	ctx, cancel := context.WithTimeout(r.stctx, lookupReadableLogsTimeout)
	defer cancel()
	ls, err := r.config.Client.Eventbus(ctx, r.config.EventBusName).ListLog(ctx)
	if err != nil {
		log.Warning(ctx, "eventbus lookup Readable eventlog error", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBusName,
			log.KeyError:        err,
		})
		return
	}
	els := make([]uint64, len(ls))
	for idx, l := range ls {
		els[idx] = l.ID()
	}
	if len(els) != len(r.elReader) {
		log.Info(ctx, "event eventlog change,will restart event eventlog reader", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBusName,
		})
		r.start(els)
		log.Info(ctx, "event eventlog change,restart event eventlog reader success", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBusName,
		})
	}
}

func (r *reader) getOffset(ctx context.Context, eventLogID vanus.ID) (uint64, error) {
	l, err := r.config.Client.Eventbus(ctx, r.config.EventBusName).GetLog(ctx, eventLogID.Uint64())
	if err != nil {
		return 0, err
	}
	offset, exist := r.config.Offset[eventLogID]
	if !exist {
		var err error
		var v int64
		switch r.config.OffsetType {
		case primitive.LatestOffset:
			if v, err = l.LatestOffset(ctx); err != nil {
				return 0, err
			}
		case primitive.EarliestOffset:
			if v, err = l.EarliestOffset(ctx); err != nil {
				return 0, err
			}
		case primitive.Timestamp:
			if v, err = l.QueryOffsetByTime(ctx, r.config.OffsetTimestamp); err != nil {
				return 0, err
			}
		}
		offset = uint64(v)
	}
	return offset, nil
}

func (r *reader) start(els []uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, el := range els {
		eventLogID := vanus.ID(el)
		if _, exist := r.elReader[eventLogID]; exist {
			continue
		}
		offset, err := r.getOffset(r.stctx, eventLogID)
		if err != nil {
			log.Error(r.stctx, "event log get offset error", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}
		l, err := r.config.Client.Eventbus(r.stctx, r.config.EventBusName).GetLog(r.stctx, eventLogID.Uint64())
		if err != nil {
			log.Error(r.stctx, "get eventlog error", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}
		elc := &eventLogReader{
			config:        r.config,
			eventLogID:    eventLogID,
			eventLogIDStr: eventLogID.String(),
			policy:        policy.NewManuallyReadPolicy(l, int64(offset)),
			events:        r.events,
			offset:        offset,
		}
		r.elReader[elc.eventLogID] = struct{}{}
		r.wg.Add(1)
		go func() {
			defer func() {
				r.wg.Done()
				log.Info(r.stctx, "event eventlog reader stop", map[string]interface{}{
					log.KeyEventlogID: elc.eventLogID,
					"offset":          elc.offset,
				})
			}()
			log.Info(r.stctx, "event eventlog reader start", map[string]interface{}{
				log.KeyEventlogID: elc.eventLogID,
			})
			elc.run(r.stctx)
		}()
	}
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
	for attempt := 0; ; attempt++ {
		lr, err := elReader.init(ctx)
		switch err {
		case nil:
		case context.Canceled:
			return
		case context.DeadlineExceeded:
			log.Warning(ctx, "eventlog reader init timeout", map[string]interface{}{
				log.KeyEventbusName: elReader.config.EventBusName,
				log.KeyEventlogID:   elReader.eventLogID,
			})
			continue
		default:
			log.Warning(ctx, "eventlog reader init error,will retry", map[string]interface{}{
				log.KeyEventbusName: elReader.config.EventBusName,
				log.KeyEventlogID:   elReader.eventLogID,
				log.KeyError:        err,
			})
			if !util.SleepWithContext(ctx, initErrSleepTime) {
				return
			}
			continue
		}
		log.Info(ctx, "eventlog reader init success", map[string]interface{}{
			log.KeyEventbusName: elReader.config.EventBusName,
			log.KeyEventlogID:   elReader.eventLogID,
			"offset":            elReader.offset,
		})
		for {
			err = elReader.readEvent(ctx, lr)
			switch err {
			case nil:
				continue
			case context.Canceled:
				return
			case eberrors.ErrOnEnd, eberrors.ErrTryAgain:
			case eberrors.ErrUnderflow:
				// todo reset offset timestamp
			default:
				log.Warning(ctx, "read event error", map[string]interface{}{
					log.KeyEventlogID: elReader.eventLogID,
					"offset":          elReader.offset,
					log.KeyError:      err,
				})
				if !util.SleepWithContext(ctx, readErrSleepTime) {
					return
				}
			}
		}
	}
}

func (elReader *eventLogReader) readEvent(ctx context.Context, lr api.BusReader) error {
	events, err := readEvents(ctx, lr, elReader.policy)
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
		elReader.policy.Forward(1)
	}
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

func readEvents(ctx context.Context, lr api.BusReader, p api.ReadPolicy) ([]*ce.Event, error) {
	timeout, cancel := context.WithTimeout(ctx, readEventTimeout)
	defer cancel()
	events, _, _, err := lr.Read(timeout, option.WithReadPolicy(p), option.WithBatchSize(int(readSize)))
	return events, err
}

func (elReader *eventLogReader) init(ctx context.Context) (api.BusReader, error) {
	lr := elReader.config.Client.Eventbus(ctx, elReader.config.EventBusName).Reader()
	return lr, nil
}
