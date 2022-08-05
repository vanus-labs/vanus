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

package timingwheel

import (
	"container/list"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUtils_Exponent(t *testing.T) {
	Convey("test 10^2 equal 100", t, func() {
		ret := exponent(time.Second, 10, 2)
		So(ret, ShouldEqual, 100*time.Second)
	})
}

func TestUtils_Add(t *testing.T) {
	Convey("test list add two elements", t, func() {
		c := &Config{}
		l := list.New()
		tw1 := newTimingWheelElement(c, nil, nil, 1, 1)
		add(l, tw1)
		tw2 := newTimingWheelElement(c, nil, nil, 2, 2)
		add(l, tw2)
		So(l.Front().Value.(*timingWheelElement).tick, ShouldEqual, 1)
		So(l.Front().Next().Value.(*timingWheelElement).tick, ShouldEqual, 2)
	})
}
