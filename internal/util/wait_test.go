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

package util

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/observability/log"
	. "github.com/smartystreets/goconvey/convey"
)

func TestWaitStartWithContext(t *testing.T) {
	Convey("start with context", t, func() {
		g := Group{}
		ctx, cancel := context.WithCancel(context.Background())
		g.StartWithContext(ctx, func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				time.Sleep(time.Millisecond)
			}
		})
		time.Sleep(time.Millisecond * 10)
		cancel()
		g.Wait()
	})
}

func TestWaitStartWithChannel(t *testing.T) {
	Convey("start with channel", t, func() {
		g := Group{}
		ch := make(chan struct{})
		g.StartWithChannel(ch, func(stopCh <-chan struct{}) {
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				time.Sleep(time.Millisecond)
			}
		})
		time.Sleep(time.Millisecond * 10)
		ch <- struct{}{}
		g.Wait()
	})
}

func TestUntilWithContext(t *testing.T) {
	Convey("util with context", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		count := int64(0)
		interval := 20 * time.Millisecond
		go UntilWithContext(ctx, func(ctx context.Context) {
			log.Info(ctx, "entrance", nil)
			atomic.AddInt64(&count, 1)
		}, interval)
		time.Sleep(10 * interval)
		cancel()
		So(atomic.LoadInt64(&count), ShouldBeGreaterThanOrEqualTo, 5)
		// waiting goroutine exit
		time.Sleep(interval)
		old := atomic.LoadInt64(&count)
		time.Sleep(10 * interval)
		So(atomic.LoadInt64(&count), ShouldEqual, old)
	})
}
