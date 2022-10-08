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

package eventlog

import (
	// standard libraries.
	"context"
	"time"

	// this project.

	"github.com/linkall-labs/vanus/client/pkg/primitive"
	"github.com/linkall-labs/vanus/client/pkg/record"
)

const (
	defaultWatchInterval = 30 * time.Second
)

type WritableSegmentWatcher struct {
	*primitive.Watcher
	ch chan *record.Segment
}

func (w *WritableSegmentWatcher) Chan() <-chan *record.Segment {
	return w.ch
}

func (w *WritableSegmentWatcher) Start() {
	go w.Watcher.Run()
}

func WatchWritableSegment(log *eventlogImpl) *WritableSegmentWatcher {
	// TODO: true watch
	ch := make(chan *record.Segment, 1)
	w := primitive.NewWatcher(defaultWatchInterval, func() {
		r, err := log.nameService.LookupWritableSegment(context.Background(), log.cfg.ID)
		if err != nil {
			// TODO: logging

			// FIXME: notify
			ch <- nil
		} else {
			ch <- r
		}
	}, func() {
		close(ch)
	})
	watcher := &WritableSegmentWatcher{
		Watcher: w,
		ch:      ch,
	}
	return watcher
}

type ReadableSegmentsWatcher struct {
	*primitive.Watcher
	ch chan []*record.Segment
}

func (w *ReadableSegmentsWatcher) Chan() <-chan []*record.Segment {
	return w.ch
}

func (w *ReadableSegmentsWatcher) Start() {
	go w.Watcher.Run()
}

func WatchReadableSegments(log *eventlogImpl) *ReadableSegmentsWatcher {
	// TODO: true watch
	ch := make(chan []*record.Segment, 1)
	w := primitive.NewWatcher(defaultWatchInterval, func() {
		rs, err := log.nameService.LookupReadableSegments(context.Background(), log.cfg.ID)
		if err != nil {
			// TODO: logging

			// FIXME: notify
			ch <- nil
		} else {
			ch <- rs
		}
	}, func() {
		close(ch)
	})
	watcher := &ReadableSegmentsWatcher{
		Watcher: w,
		ch:      ch,
	}
	return watcher
}
