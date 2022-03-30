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
		tk := time.NewTicker(time.Second)
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
		log.Warning(ctx, "eventbus lookup Readable log error", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBus,
			log.KeyError:        err,
		})
		return
	}
	if len(els) != len(r.elReader) {
		log.Info(ctx, "event log change,will restart event log reader", map[string]interface{}{
			log.KeyEventbusName: r.config.EventBus,
		})
		r.start(els)
		log.Info(ctx, "event log change,restart event log reader success", map[string]interface{}{
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
	for _, el := range els {
		if _, exist := r.elReader[el.VRN]; exist {
			continue
		}
		elc := &eventLogReader{
			config:   r.config,
			eventLog: el.VRN,
			events:   r.events,
			offset:   r.getOffset(el.VRN),
		}
		r.elReader[el.VRN] = struct{}{}
		r.wg.Add(1)
		go func() {
			defer func() {
				r.wg.Done()
				log.Info(r.stctx, "event log reader stop", map[string]interface{}{
					log.KeyEventlogID: elc.eventLog,
				})
			}()
			log.Info(r.stctx, "event log reader start", map[string]interface{}{
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
	printLogCount := 3
	for attempt := 0; ; attempt++ {
		lr, err := elReader.init(ctx)
		switch err {
		case nil:
		case context.Canceled:
			return
		case context.DeadlineExceeded:
			log.Warning(ctx, "event log reader init timeout", map[string]interface{}{
				log.KeyEventlogID: elReader.eventLog,
			})
			continue
		default:
			if attempt%printLogCount == 0 {
				log.Info(ctx, "event log reader init error,will retry", map[string]interface{}{
					log.KeyEventbusName: elReader.config.EventBus,
					log.KeyError:        err,
				})
			}
			if !util.SleepWithContext(ctx, time.Second*2) {
				return
			}
			continue
		}
		printLogCount = 0
		sleepCnt := 0
		for {
			events, err := readEvents(ctx, lr)
			switch err {
			case nil:
				for i := range events {
					elReader.offset++
					elReader.sendEvent(ctx, info.EventOffset{Event: events[i], OffsetInfo: pInfo.OffsetInfo{EventLog: elReader.eventLog, Offset: elReader.offset}})
				}
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

func (elReader *eventLogReader) sendEvent(ctx context.Context, event info.EventOffset) {
	select {
	case elReader.events <- event:
		return
	case <-ctx.Done():
		return
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
