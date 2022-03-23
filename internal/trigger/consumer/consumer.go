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

package consumer

import (
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	eberrors "github.com/linkall-labs/eventbus-go/pkg/errors"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"io"
	"sync"
	"time"

	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
)

type Config struct {
	EventBus        string
	SubId           string
	TriggerCtrlAddr string
}

type Consumer struct {
	config        Config
	offsetManager *offset.SubscriptionOffset
	elConsumer    map[string]*EventLogConsumer
	events        chan<- *info.EventRecord
	offset        map[string]int64
	cancel        context.CancelFunc
	stop          context.CancelFunc
	stctx         context.Context
	wg            sync.WaitGroup
	lock          sync.Mutex
}

func NewConsumer(config Config, offset map[string]int64, offsetManager *offset.SubscriptionOffset, events chan<- *info.EventRecord) *Consumer {
	c := &Consumer{
		config:        config,
		offset:        offset,
		offsetManager: offsetManager,
		events:        events,
		elConsumer:    map[string]*EventLogConsumer{},
		cancel:        func() {},
	}
	c.stctx, c.stop = context.WithCancel(context.Background())
	return c
}

func (c *Consumer) Close() {
	c.cancel()
	c.stop()
	c.wg.Wait()
	log.Info(c.stctx, "consumer closed", map[string]interface{}{
		log.KeyEventbusName:   c.config.EventBus,
		log.KeySubscriptionID: c.config.SubId,
	})
}
func (c *Consumer) Start(parent context.Context) error {
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-c.stctx.Done():
				return
			case <-tk.C:
				c.checkEventLogChange()
			}
		}
	}()
	return nil
}

func (c *Consumer) checkEventLogChange() {
	ctx, cancel := context.WithTimeout(c.stctx, 5*time.Second)
	defer cancel()
	els, err := eb.LookupReadableLogs(ctx, c.config.SubId)
	if err != nil {
		if err == context.Canceled {
			return
		}
		log.Warning(ctx, "eventbus lookup Readable log error", map[string]interface{}{
			log.KeyEventbusName:   c.config.EventBus,
			log.KeySubscriptionID: c.config.SubId,
			log.KeyError:          err,
		})
		return
	}
	if len(els) != len(c.elConsumer) {
		log.Info(ctx, "event log change,will restart event log consumer", map[string]interface{}{
			log.KeyEventbusName: c.config.EventBus,
		})
		c.start(els)
		log.Info(ctx, "event log change,restart event log consumer success", map[string]interface{}{
			log.KeyEventbusName: c.config.EventBus,
		})
	}
}

func (c *Consumer) getOffset(el string) int64 {
	offset, exist := c.offset[el]
	if !exist {
		offset = 0
	}
	return offset
}

func (c *Consumer) start(els []*record.EventLog) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel()
	c.cancel = cancel
	for k := range c.elConsumer {
		delete(c.elConsumer, k)
	}
	for _, el := range els {
		elc := &EventLogConsumer{
			config:        c.config,
			eventLog:      el.VRN,
			events:        c.events,
			offset:        c.getOffset(el.VRN),
			offsetManager: c.offsetManager,
		}
		c.elConsumer[el.VRN] = elc
		c.wg.Add(1)
		go func() {
			defer func() {
				c.wg.Done()
				log.Info(ctx, "event log consumer stop", map[string]interface{}{
					log.KeySubscriptionID: elc.config.SubId,
					log.KeyEventlogID:     elc.eventLog,
				})
			}()
			log.Info(ctx, "event log consumer start", map[string]interface{}{
				log.KeySubscriptionID: elc.config.SubId,
				log.KeyEventlogID:     elc.eventLog,
			})
			elc.run(ctx)
			c.offset[elc.eventLog] = elc.offset
		}()
	}
}

type EventLogConsumer struct {
	config        Config
	eventLog      string
	events        chan<- *info.EventRecord
	offsetManager *offset.SubscriptionOffset
	offset        int64
}

func (lc *EventLogConsumer) run(ctx context.Context) {
	for {
		lr, _, err := lc.init(ctx)
		if err != nil {
			log.Warning(ctx, "event log consumer init error,will retry", map[string]interface{}{
				log.KeyEventbusName:   lc.config.EventBus,
				log.KeySubscriptionID: lc.config.SubId,
				log.KeyError:          err,
			})
			if !util.Sleep(ctx, time.Second*2) {
				return
			}
			continue
		}
		sleepCnt := 0
		for {
			select {
			case <-ctx.Done():
				lr.Close()
				return
			default:
			}
			events, err := readEvents(ctx, lr)
			switch err {
			case nil:
				for i := range events {
					lc.offset++
					lc.sendEvent(ctx, &info.EventRecord{Event: events[i], OffsetInfo: info.OffsetInfo{EventLog: lc.eventLog, Offset: lc.offset}})
				}
				sleepCnt = 0
				continue
			case io.EOF:
				sleepCnt = 0
				continue
			case context.Canceled:
				lr.Close()
				return
			case context.DeadlineExceeded:
				log.Warning(ctx, "readEvents error", map[string]interface{}{
					log.KeySubscriptionID: lc.config.SubId,
					log.KeyEventlogID:     lc.eventLog,
					"offset":              lc.offset,
					log.KeyError:          err,
				})
				continue
			case eberrors.ErrOnEnd:
			case eberrors.ErrUnderflow:
			default:
				//other error

				log.Warning(ctx, "read event error", map[string]interface{}{
					log.KeySubscriptionID: lc.config.SubId,
					log.KeyEventlogID:     lc.eventLog,
					"offset":              lc.offset,
					log.KeyError:          err,
				})
			}
			sleepCnt++
			if !util.Sleep(ctx, util.Backoff(sleepCnt, 2*time.Second)) {
				lr.Close()
				return
			}
		}
	}
}

func (el *EventLogConsumer) sendEvent(ctx context.Context, event *info.EventRecord) {
	select {
	case el.events <- event:
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

func (lc *EventLogConsumer) init(ctx context.Context) (eventlog.LogReader, int64, error) {
	lr, err := eb.OpenLogReader(lc.eventLog)
	if err != nil {
		return nil, 0, errors.Wrap(err, "open log reader error")
	}
	lc.offsetManager.RegisterEventLog(lc.eventLog, lc.offset)
	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	newOffset, err := lr.Seek(timeout, lc.offset, io.SeekStart)
	if err != nil {
		//todo overflow need reset offset
		lr.Close()
		return nil, 0, errors.Wrapf(err, "seek error offset %d", lc.offset)
	}
	return lr, newOffset, nil
}
