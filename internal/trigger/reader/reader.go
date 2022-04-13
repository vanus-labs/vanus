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

package reader

import (
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	eberrors "github.com/linkall-labs/eventbus-go/pkg/errors"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"io"
	"sync"
	"time"

	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/eventlog"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
)

type Config struct {
	EventBus string
	SubId    string
}
type EventLogOffset map[string]int64

type Reader struct {
	config   Config
	elReader map[string]struct{}
	events   chan<- info.EventOffset
	offset   EventLogOffset
	stop     context.CancelFunc
	stctx    context.Context
	wg       sync.WaitGroup
	lock     sync.Mutex
}

func NewReader(config Config, offset EventLogOffset, events chan<- info.EventOffset) *Reader {
	r := &Reader{
		config:   config,
		offset:   offset,
		events:   events,
		elReader: map[string]struct{}{},
	}
	r.stctx, r.stop = context.WithCancel(context.Background())
	return r
}

func (r *Reader) Close() {
	r.stop()
	r.wg.Wait()
	log.Info(r.stctx, "reader closed", map[string]interface{}{
		log.KeyEventbusName: r.config.EventBus,
	})
}
func (r *Reader) Start() error {
	go func() {
		r.checkEventLogChange()
		tk := time.NewTicker(30 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-r.stctx.Done():
				return
			case <-tk.C:
				r.checkEventLogChange()
			}
		}
	}()
	return nil
}

func (r *Reader) checkEventLogChange() {
	ctx, cancel := context.WithTimeout(r.stctx, 5*time.Second)
	defer cancel()
	els, err := eb.LookupReadableLogs(ctx, r.config.EventBus)
	if err != nil {
		if err == context.Canceled {
			return
		}
		log.Warning(ctx, "eventbus lookup Readable eventlog error", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBus,
			log.KeyError:        err,
		})
		return
	}
	if len(els) != len(r.elReader) {
		log.Info(ctx, "event eventlog change,will restart event eventlog reader", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBus,
		})
		r.start(els)
		log.Info(ctx, "event eventlog change,restart event eventlog reader success", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBus,
		})
	}
}

func (r *Reader) getOffset(el string) int64 {
	offset, exist := r.offset[el]
	if !exist {
		offset = 0
	}
	return offset
}

func (r *Reader) start(els []*record.EventLog) {
	r.lock.Lock()
	r.lock.Unlock()
	for _, el := range els {
		eventLog := el.VRN
		if _, exist := r.elReader[eventLog]; exist {
			continue
		}
		elc := &eventLogReader{
			config:   r.config,
			eventLog: eventLog,
			events:   r.events,
			offset:   r.getOffset(eventLog),
		}
		r.elReader[eventLog] = struct{}{}
		r.wg.Add(1)
		go func() {
			defer func() {
				r.wg.Done()
				log.Info(r.stctx, "event eventlog reader stop", map[string]interface{}{
					log.KeyEventlogID: elc.eventLog,
				})
			}()
			log.Info(r.stctx, "event eventlog reader start", map[string]interface{}{
				log.KeyEventlogID: elc.eventLog,
			})
			elc.run(r.stctx)
			r.offset[elc.eventLog] = elc.offset
		}()
	}
}

type eventLogReader struct {
	config   Config
	eventLog string
	events   chan<- info.EventOffset
	offset   int64
}

func (elReader *eventLogReader) run(ctx context.Context) {
	for attempt := 0; ; attempt++ {
		lr, err := elReader.init(ctx)
		switch err {
		case nil:
		case context.Canceled:
			return
		case context.DeadlineExceeded:
			log.Warning(ctx, "event eventlog reader init timeout", map[string]interface{}{
				log.KeyEventlogID: elReader.eventLog,
			})
			continue
		default:
			log.Info(ctx, "event eventlog reader init error,will retry", map[string]interface{}{
				log.KeyEventbusName: elReader.config.EventBus,
				log.KeyError:        err,
			})
			if !util.SleepWithContext(ctx, time.Second*5) {
				return
			}
			continue
		}
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
					log.KeyEventlogID: elReader.eventLog,
					"offset":          elReader.offset,
				})
				continue
			case errors.ErrReadNoEvent:
			case eberrors.ErrOnEnd:
			case eberrors.ErrUnderflow:
			default:
				log.Warning(ctx, "read event error", map[string]interface{}{
					log.KeyEventlogID: elReader.eventLog,
					"offset":          elReader.offset,
					log.KeyError:      err,
				})
			}
			sleepCnt++
			if !util.SleepWithContext(ctx, util.Backoff(sleepCnt, 2*time.Second)) {
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
		elReader.offset++
		e := info.EventOffset{Event: events[i], OffsetInfo: pInfo.OffsetInfo{EventLog: elReader.eventLog, Offset: elReader.offset}}
		if err = elReader.sendEvent(ctx, e); err != nil {
			return err
		}
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
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return lr.Read(timeout, 5)
}

func (elReader *eventLogReader) init(ctx context.Context) (eventlog.LogReader, error) {
	lr, err := eb.OpenLogReader(elReader.eventLog)
	if err != nil {
		return nil, err
	}
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = lr.Seek(timeout, elReader.offset, io.SeekStart)
	if err != nil {
		//todo overflow need reset offset
		lr.Close()
		return nil, err
	}
	return lr, nil
}
