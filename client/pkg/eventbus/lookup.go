// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventbus

import (
	// standard libraries.
	"context"
	"time"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability/log"

	// this project.
	"github.com/vanus-labs/vanus/client/pkg/primitive"
	"github.com/vanus-labs/vanus/client/pkg/record"
)

type WritableLogsResult struct {
	Eventlogs []*record.Eventlog
	Err       error
}

type WritableLogsWatcher struct {
	*primitive.Watcher
	ch chan *WritableLogsResult
}

func (w *WritableLogsWatcher) Chan() <-chan *WritableLogsResult {
	return w.ch
}

func (w *WritableLogsWatcher) Start() {
	go w.Watcher.Run()
}

func WatchWritableLogs(bus *eventbus) *WritableLogsWatcher {
	ch := make(chan *WritableLogsResult, 1)
	w := primitive.NewWatcher(30*time.Second, func() {
		rs, err := bus.nameService.LookupWritableLogs(context.Background(), bus.cfg.ID)
		log.Debug().Err(err).
			Uint64("eventbus_id", bus.cfg.ID).
			Interface("logs", rs).
			Msg("lookup writable logs")
		ch <- &WritableLogsResult{
			Eventlogs: rs,
			Err:       err,
		}
	}, func() {
		close(ch)
	})
	watcher := &WritableLogsWatcher{
		Watcher: w,
		ch:      ch,
	}

	return watcher
}

type ReadableLogsResult struct {
	Eventlogs []*record.Eventlog
	Err       error
}

type ReadableLogsWatcher struct {
	*primitive.Watcher
	ch chan *ReadableLogsResult
}

func (w *ReadableLogsWatcher) Chan() <-chan *ReadableLogsResult {
	return w.ch
}

func (w *ReadableLogsWatcher) Start() {
	go w.Watcher.Run()
}

func WatchReadableLogs(bus *eventbus) *ReadableLogsWatcher {
	ch := make(chan *ReadableLogsResult, 1)
	w := primitive.NewWatcher(30*time.Second, func() {
		rs, err := bus.nameService.LookupReadableLogs(context.Background(), bus.cfg.ID)
		log.Debug().Err(err).
			Uint64("eventbus_id", bus.cfg.ID).
			Interface("logs", rs).
			Msg("lookup readable logs")
		ch <- &ReadableLogsResult{
			Eventlogs: rs,
			Err:       err,
		}
	}, func() {
		close(ch)
	})
	watcher := &ReadableLogsWatcher{
		Watcher: w,
		ch:      ch,
	}

	return watcher
}
