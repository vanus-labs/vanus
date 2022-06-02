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
	"time"

	vdr "github.com/linkall-labs/vanus/client/internal/vanus/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

var (
	ns = newNameService()
)

type WritableSegmentWatcher struct {
	*primitive.Watcher
	ch chan *vdr.LogSegment
}

func (w *WritableSegmentWatcher) Chan() <-chan *vdr.LogSegment {
	return w.ch
}

func (w *WritableSegmentWatcher) Start() {
	go w.Watcher.Run()
}

func WatchWritableSegment(eventlog *discovery.VRN) (*WritableSegmentWatcher, error) {
	// TODO: true watch
	ch := make(chan *vdr.LogSegment, 1)
	w := primitive.NewWatcher(30*time.Second, func() {
		r, err := ns.LookupWritableSegment(eventlog)
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
	return watcher, nil
}

type ReadableSegmentsWatcher struct {
	*primitive.Watcher
	ch chan []*vdr.LogSegment
}

func (w *ReadableSegmentsWatcher) Chan() <-chan []*vdr.LogSegment {
	return w.ch
}

func (w *ReadableSegmentsWatcher) Start() {
	go w.Watcher.Run()
}

func WatchReadableSegments(eventlog *discovery.VRN) (*ReadableSegmentsWatcher, error) {
	// TODO: true watch
	ch := make(chan []*vdr.LogSegment, 1)
	w := primitive.NewWatcher(30*time.Second, func() {
		rs, err := ns.LookupReadableSegments(eventlog)
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
	return watcher, nil
}
