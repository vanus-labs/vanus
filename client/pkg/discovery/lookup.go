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

package discovery

import (
	// standard libraries.
	"context"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

type WritableLogsResult struct {
	Eventlogs []*record.EventLog
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

func WatchWritableLogs(eventbus *VRN) (*WritableLogsWatcher, error) {
	ns := Find(eventbus.Scheme)
	if ns == nil {
		return nil, errors.ErrNotSupported
	}

	// TODO: true watch
	ch := make(chan *WritableLogsResult, 1)
	w := primitive.NewWatcher(30*time.Second, func() {
		rs, err := ns.LookupWritableLogs(context.Background(), eventbus)
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

	return watcher, nil
}

func LookupWritableLogs(ctx context.Context, bus *VRN) ([]*record.EventLog, error) {
	ns := Find(bus.Scheme)
	if ns == nil {
		return nil, errors.ErrNotSupported
	}
	return ns.LookupWritableLogs(ctx, bus)
}

func LookupReadableLogs(ctx context.Context, bus *VRN) ([]*record.EventLog, error) {
	ns := Find(bus.Scheme)
	if ns == nil {
		return nil, errors.ErrNotSupported
	}
	return ns.LookupReadableLogs(ctx, bus)
}
