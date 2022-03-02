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

type Consumer struct {
	ebVrn         string
	sub           string
	offsetManager *EventLogOffset
	elConsumer    map[string]EventLogConsumer
	handler       EventHandler
	cancel        context.CancelFunc
	stop          context.CancelFunc
	wg            sync.WaitGroup
	lock          sync.Mutex
}

func NewConsumer(ebVRN, sub string, offsetManager *EventLogOffset, handler EventHandler) *Consumer {
	return &Consumer{
		ebVrn:         ebVRN,
		sub:           sub,
		offsetManager: offsetManager,
		handler:       handler,
		elConsumer:    map[string]EventLogConsumer{},
	}
}

func (c *Consumer) Close() {
	c.stop()
	c.cancel()
	c.wg.Wait()
}
func (c *Consumer) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	c.stop = cancel
	els, err := eb.LookupReadableLogs(c.ebVrn)
	if err != nil {
		return errors.Wrapf(err, "eb")
	}
	c.start(els)
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				c.checkEventLogChange()
			}
		}
	}()
	return nil
}

func (c *Consumer) checkEventLogChange() {
	els, err := eb.LookupReadableLogs(c.ebVrn)
	if err != nil {
		log.Error("eventbus lookup Readable log error", map[string]interface{}{
			"ebVrn":      c.ebVrn,
			log.KeyError: err,
		})
		return
	}
	if len(els) != len(c.elConsumer) {
		log.Info("event log change,will restart event log consumer", map[string]interface{}{
			"ebVrn": c.ebVrn,
		})
		c.start(els)
		log.Info("event log change,restart event log consumer success", map[string]interface{}{
			"ebVrn": c.ebVrn,
		})
	}
}

func (c *Consumer) start(els []*record.EventLog) {
	ctx, cancel := context.WithCancel(context.Background())
	if c.cancel != nil {
		c.cancel()
	}
	c.cancel = cancel
	for k := range c.elConsumer {
		delete(c.elConsumer, k)
	}
	for _, el := range els {
		elc := EventLogConsumer{
			elVrn:         el.VRN,
			sub:           c.sub,
			offsetManager: c.offsetManager,
			handler:       c.handler,
		}
		c.elConsumer[el.VRN] = elc
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			elc.run(ctx)
		}()
	}
}

type EventHandler func(context.Context, *info.EventRecord) error

type EventLogConsumer struct {
	elVrn         string
	sub           string
	offsetManager *EventLogOffset
	handler       EventHandler
	offset        int64
}

func (lc *EventLogConsumer) run(ctx context.Context) {
	for {
		lr, offset, err := lc.init()
		if err != nil {
			log.Warning("event log consumer init error,will retry", map[string]interface{}{
				"sub":        lc.sub,
				"elVrn":      lc.elVrn,
				log.KeyError: err,
			})
			if !util.Sleep(ctx, time.Second*2) {
				return
			}
			continue
		}
		for {
			select {
			case <-ctx.Done():
				log.Info("event log consumer stop", map[string]interface{}{
					"sub":    lc.sub,
					"elVrn":  lc.elVrn,
					"offset": offset,
				})
				lr.Close()
				return
			default:
			}
			events, err := lr.Read(2)
			if err != nil {
				log.Warning("read error", map[string]interface{}{
					"sub":        lc.sub,
					"elVrn":      lc.elVrn,
					"offset":     offset,
					log.KeyError: err,
				})
				continue
			}
			if len(events) == 0 {
				time.Sleep(time.Millisecond * 100)
			}
			for i := range events {
				offset++
				lc.handler(context.Background(), &info.EventRecord{Event: events[i], EventLog: lc.elVrn, Offset: offset})
			}
		}
	}
}

func (lc *EventLogConsumer) init() (eventlog.LogReader, int64, error) {
	lr, err := eb.OpenLogReader(lc.elVrn)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "sub %s el %s open log reader error", lc.sub, lc.elVrn)
	}
	offset, err := lc.offsetManager.RegisterEventLog(lc.elVrn, 0)
	if err != nil {
		lr.Close()
		return nil, 0, errors.Wrapf(err, "sub %s el %s offset register event log error", lc.sub, lc.elVrn)
	}
	lc.offset = offset
	newOffset, err := lr.Seek(offset, io.SeekStart)
	if err != nil {
		lr.Close()
		return nil, 0, errors.Wrapf(err, "sub %s el %s seek offset %d error", lc.sub, lc.elVrn, offset)
	}
	return lr, newOffset, nil
}
