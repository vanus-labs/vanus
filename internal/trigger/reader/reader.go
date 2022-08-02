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
	"io"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	eberrors "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
)

const (
	checkEventLogPeriod       = 30 * time.Second
	lookupReadableLogsTimeout = 5 * time.Second
	readerSeekTimeout         = 5 * time.Second
	readEventTimeout          = 5 * time.Second
	initErrSleepTime          = 5 * time.Second
	readErrSleepTime          = 2 * time.Second
	readSize                  = 5
)

type Config struct {
	EventBusName    string
	EventBusVRN     string
	SubscriptionID  vanus.ID
	Offset          EventLogOffset
	OffsetType      primitive.OffsetType
	OffsetTimestamp int64

	CheckEventLogPeriod time.Duration
}
type EventLogOffset map[vanus.ID]uint64

type Reader interface {
	Start() error
	GetOffsetByTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error)
	Close()
}

type reader struct {
	config   Config
	elReader map[vanus.ID]string
	events   chan<- info.EventOffset
	stop     context.CancelFunc
	stctx    context.Context
	wg       sync.WaitGroup
	lock     sync.Mutex
}

func NewReader(config Config, events chan<- info.EventOffset) Reader {
	if config.CheckEventLogPeriod <= 0 {
		config.CheckEventLogPeriod = checkEventLogPeriod
	}
	if config.Offset == nil {
		config.Offset = map[vanus.ID]uint64{}
	}
	r := &reader{
		config:   config,
		events:   events,
		elReader: make(map[vanus.ID]string),
	}
	r.stctx, r.stop = context.WithCancel(context.Background())
	return r
}

func (r *reader) GetOffsetByTimestamp(ctx context.Context, timestamp int64) (pInfo.ListOffsetInfo, error) {
	offsets := make(pInfo.ListOffsetInfo, 0, len(r.elReader))
	for id, elVRN := range r.elReader {
		offset, err := eb.LookupLogOffset(ctx, elVRN, timestamp)
		if err != nil {
			return offsets, err
		}
		offsets = append(offsets, pInfo.OffsetInfo{
			EventLogID: id,
			Offset:     uint64(offset),
		})
	}

	return pInfo.ListOffsetInfo{}, nil
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
	tk := time.NewTicker(r.config.CheckEventLogPeriod)
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
	els, err := eb.LookupReadableLogs(ctx, r.config.EventBusVRN)
	if err != nil {
		log.Warning(ctx, "eventbus lookup Readable eventlog error", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBusName,
			log.KeyError:        err,
		})
		return
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

func (r *reader) getOffset(ctx context.Context, eventLogID vanus.ID, elVRN string) (uint64, error) {
	offset, exist := r.config.Offset[eventLogID]
	if !exist {
		switch r.config.OffsetType {
		case primitive.LatestOffset:
			v, err := eb.LookupLatestLogOffset(ctx, elVRN)
			if err != nil {
				return 0, err
			}
			offset = uint64(v)
		case primitive.EarliestOffset:
			v, err := eb.LookupEarliestLogOffset(ctx, elVRN)
			if err != nil {
				return 0, err
			}
			offset = uint64(v)
		case primitive.Timestamp:
			v, err := eb.LookupLogOffset(ctx, elVRN, r.config.OffsetTimestamp)
			if err != nil {
				return 0, err
			}
			offset = uint64(v)
		}
	}
	return offset, nil
}

func (r *reader) start(els []*record.EventLog) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, el := range els {
		vrn, err := discovery.ParseVRN(el.VRN)
		if err != nil {
			log.Error(r.stctx, "event log vrn parse error", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}

		eventLogID := vanus.ID(vrn.ID)
		if _, exist := r.elReader[eventLogID]; exist {
			continue
		}
		offset, err := r.getOffset(r.stctx, eventLogID, el.VRN)
		if err != nil {
			log.Error(r.stctx, "event log get offset error", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}
		elc := &eventLogReader{
			config:      r.config,
			eventLogVrn: el.VRN,
			eventLogID:  eventLogID,
			events:      r.events,
			offset:      offset,
		}
		r.elReader[elc.eventLogID] = el.VRN
		r.wg.Add(1)
		go func() {
			defer func() {
				r.wg.Done()
				log.Info(r.stctx, "event eventlog reader stop", map[string]interface{}{
					log.KeyEventlogID: elc.eventLogVrn,
					"offset":          elc.offset,
				})
			}()
			log.Info(r.stctx, "event eventlog reader start", map[string]interface{}{
				log.KeyEventlogID: elc.eventLogVrn,
			})
			elc.run(r.stctx)
		}()
	}
}

type eventLogReader struct {
	config      Config
	eventLogID  vanus.ID
	eventLogVrn string
	events      chan<- info.EventOffset
	offset      uint64
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
				log.KeyEventlogID:   elReader.eventLogVrn,
			})
			continue
		default:
			log.Info(ctx, "eventlog reader init error,will retry", map[string]interface{}{
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
		sleepCnt := 0
		for {
			err = elReader.readEvent(ctx, lr)
			switch err {
			case nil:
				sleepCnt = 0
				continue
			case context.Canceled:
				lr.Close()
				return
			case context.DeadlineExceeded:
				log.Warning(ctx, "readEvents timeout", map[string]interface{}{
					log.KeyEventlogID: elReader.eventLogVrn,
					"offset":          elReader.offset,
				})
				continue
			case errors.ErrReadNoEvent:
			case eberrors.ErrOnEnd:
			case eberrors.ErrUnderflow:
			default:
				log.Warning(ctx, "read event error", map[string]interface{}{
					log.KeyEventlogID: elReader.eventLogVrn,
					"offset":          elReader.offset,
					log.KeyError:      err,
				})
			}
			sleepCnt++
			if !util.SleepWithContext(ctx, util.Backoff(sleepCnt, readErrSleepTime)) {
				lr.Close()
				return
			}
		}
	}
}

func (elReader *eventLogReader) readEvent(ctx context.Context, lr eventlog.LogReader) error {
	events, err := readEvents(ctx, lr)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return errors.ErrReadNoEvent
	}
	for i := range events {
		e := events[i]
		e.Time()
		offsetByte, _ := e.Extensions()[eventlog.XVanusLogOffset].([]byte)
		offset := binary.BigEndian.Uint64(offsetByte)
		eo := info.EventOffset{Event: events[i], OffsetInfo: pInfo.OffsetInfo{
			EventLogID: elReader.eventLogID,
			Offset:     offset,
		}}
		if err = elReader.sendEvent(ctx, eo); err != nil {
			return err
		}
		elReader.offset = offset
	}
	return nil
}
func (elReader *eventLogReader) sendEvent(ctx context.Context, event info.EventOffset) error {
	select {
	case elReader.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func readEvents(ctx context.Context, lr eventlog.LogReader) ([]*ce.Event, error) {
	timeout, cancel := context.WithTimeout(ctx, readEventTimeout)
	defer cancel()
	return lr.Read(timeout, readSize)
}

func (elReader *eventLogReader) init(ctx context.Context) (eventlog.LogReader, error) {
	lr, err := eb.OpenLogReader(elReader.eventLogVrn)
	if err != nil {
		return nil, err
	}
	timeout, cancel := context.WithTimeout(ctx, readerSeekTimeout)
	defer cancel()
	_, err = lr.Seek(timeout, int64(elReader.offset), io.SeekStart)
	if err != nil {
		// todo overflow need reset offset.
		lr.Close()
		return nil, err
	}
	return lr, nil
}
