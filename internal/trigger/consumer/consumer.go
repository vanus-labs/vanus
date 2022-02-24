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
	"errors"
	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/eventlog"
	"github.com/linkall-labs/vanus/observability/log"
	"io"
	"sync"
	"time"
)

type Consumer struct {
	ebVrn      string
	sub        string
	elConsumer map[string]EventLogConsumer
	handler    EventHandler
	cancel     context.CancelFunc
	stop       context.CancelFunc
	ctx        context.Context
	wg         sync.WaitGroup
	lock       sync.Mutex
}

func NewConsumer(ebVRN, sub string, handler EventHandler) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	offset := NewEventLogOffset(ctx, sub)
	AddEventLogOffset(sub, offset)
	return &Consumer{
		ebVrn:      ebVRN,
		sub:        sub,
		handler:    handler,
		elConsumer: map[string]EventLogConsumer{},
		stop:       cancel,
		ctx:        ctx,
	}
}

func (c *Consumer) Close() {
	c.cancel()
	c.stop()
	RemoveEventLogOffset(c.sub)
	c.wg.Wait()
}
func (c *Consumer) Start() error {
	els, err := eb.LookupReadableLog(c.ebVrn)
	if err != nil {
		return err
	}
	c.start(els)
	go func() {
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-tk.C:
				c.checkEventLogChange()
			}
		}
	}()
	return nil
}

func (c *Consumer) checkEventLogChange() {
	els, err := eb.LookupReadableLog(c.ebVrn)
	if err != nil {
		log.Error("lookup Readable log error", map[string]interface{}{"ebVrn": c.ebVrn, "error": err})
		return
	}
	if len(els) != len(c.elConsumer) {
		c.start(els)
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
			elVrn:   el.VRN,
			sub:     c.sub,
			ctx:     ctx,
			handler: c.handler,
		}
		c.elConsumer[el.VRN] = elc
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			elc.run()
		}()
	}
}

type EventHandler func(context.Context, *EventRecord) error

type EventLogConsumer struct {
	elVrn   string
	sub     string
	ctx     context.Context
	handler EventHandler
	offset  int64
}

func (lc *EventLogConsumer) run() {
	for {
		select {
		case <-lc.ctx.Done():
			log.Info("event log consumer stop", nil)
			return
		default:
		}
		lr, offset, err := lc.init()
		if err != nil {
			log.Warning("event log consumer init error", map[string]interface{}{"elVrn": lc.elVrn, "error": err})
			time.Sleep(time.Second * 10)
			continue
		}
		for {
			select {
			case <-lc.ctx.Done():
				log.Info("event log consumer stop", nil)
				return
			default:
			}
			events, err := lr.Read(2)
			if err != nil {
				log.Warning("read error", map[string]interface{}{"error": err})
			}
			if len(events) == 0 {
				time.Sleep(time.Millisecond * 100)
			}
			for i := range events {
				offset++
				lc.handler(context.Background(), &EventRecord{Event: events[i], EventLog: lc.elVrn, Offset: offset})
			}
		}
	}
}

func (lc *EventLogConsumer) init() (eventlog.LogReader, int64, error) {
	lr, err := eb.OpenLogReader(lc.elVrn)
	if err != nil {
		log.Info("open log reader error", map[string]interface{}{"error": err})
		return nil, 0, errors.New("open log reader error")
	}
	offset, err := GetEventLogOffset(lc.sub).RegisterEventLog(lc.elVrn)
	if err != nil {
		lr.Close()
		log.Info("offset register event log error", map[string]interface{}{"error": err})
		return nil, 0, errors.New("offset register event log error")
	}
	lc.offset = offset
	newOffset, err := lr.Seek(offset, io.SeekStart)
	if err != nil {
		lr.Close()
		log.Info("log reader seek error", map[string]interface{}{"error": err})
		return nil, 0, errors.New("log reader seek error")
	}
	return lr, newOffset, nil
}

type EventRecord struct {
	Event    *ce.Event
	EventLog string
	Offset   int64
}
