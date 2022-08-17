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
	timer := NewBackoffTimer(200*time.Millisecond, 600*time.Millisecond)

	Convey("test backoff timer", t, func() {
		ctx := context.Background()
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(300 * time.Millisecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(300 * time.Millisecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(200 * time.Millisecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		time.Sleep(500 * time.Millisecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(200 * time.Millisecond)
		So(timer.CanTry(), ShouldBeTrue)

		timer.SuccessHit(ctx)
		So(timer.CanTry(), ShouldBeTrue)

		timer.FailedHit(ctx)
		time.Sleep(100 * time.Millisecond)
		So(timer.CanTry(), ShouldBeFalse)

		time.Sleep(200 * time.Millisecond)
		So(timer.CanTry(), ShouldBeTrue)
	})
}
