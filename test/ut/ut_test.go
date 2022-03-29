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

package ut

import (
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestFoo(t *testing.T) {
	Convey("test gomock framework", t, func() {
		Convey("test mock", func() {
			ctrl := gomock.NewController(t)

			// Assert that Bar() is invoked.
			m := NewMockFoo(ctrl)

			// Asserts that the first and only call to Bar() is passed 99.
			// Anything else will fail.
			m.
				EXPECT().
				Bar(99).
				Return(101)

			SUT(m)
		})

		Convey("test stub", func() {
			ctrl := gomock.NewController(t)

			m := NewMockFoo(ctrl)

			// Does not make any assertions. Executes the anonymous functions and returns
			// its result when Bar is invoked with 99.
			m.
				EXPECT().
				Bar(gomock.Eq(99)).
				DoAndReturn(func(_ int) int {
					time.Sleep(1 * time.Second)
					return 101
				}).
				AnyTimes()

			// Does not make any assertions. Returns 103 when Bar is invoked with 101.
			m.
				EXPECT().
				Bar(gomock.Eq(101)).
				Return(103).
				AnyTimes()

			SUT(m)
		})
	})
}

func TestStub1(t *testing.T) {
	Convey("test function stub", t, func() {
		Convey("cases", func() {
			So(doubleInt(2), ShouldEqual, 4)
			stubs := gostub.Stub(&doubleInt, func(i int) int {
				return i * 3
			})
			defer stubs.Reset()
			So(doubleInt(2), ShouldNotEqual, 4)
			So(doubleInt(2), ShouldEqual, 6)
			gostub.StubFunc(&doubleInt, 10)
			So(doubleInt(2), ShouldNotEqual, 6)
			So(doubleInt(2), ShouldEqual, 10)
		})
	})
}
