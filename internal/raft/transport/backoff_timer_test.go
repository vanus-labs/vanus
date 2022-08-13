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

package transport

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_BackoffTimer(t *testing.T) {
	timer := NewBackoffTimer(0.2e6, 0.6e6)

	Convey("test backoff timer", t, func() {
		ctx := context.Background()
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(0.3e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(0.3e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(0.2e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		time.Sleep(0.5e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(0.2e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.SuccessHit(ctx)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		time.Sleep(0.1e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(0.2e6 * time.Microsecond)
		So(timer.CanTry(), ShouldBeTrue)
	})
}
