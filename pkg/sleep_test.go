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

package pkg

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSleepWithContext(t *testing.T) {
	Convey("sleep with context", t, func() {
		ctx := context.Background()
		b := SleepWithContext(ctx, 0)
		So(b, ShouldBeTrue)
		b = SleepWithContext(ctx, time.Millisecond)
		So(b, ShouldBeTrue)
		ctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			b = SleepWithContext(ctx, time.Minute)
		}()
		cancel()
		wg.Wait()
		So(b, ShouldBeFalse)
	})
}
