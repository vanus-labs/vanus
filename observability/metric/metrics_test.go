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

package metric

import (
	stdCtx "context"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestInit(t *testing.T) {
	Convey("test init metrics provider", t, func() {
		// TODO wait to implement this method
		Init(Config{})
	})
}

func TestNewMetricKey(t *testing.T) {
	Convey("test NewMetricKey method", t, func() {
		key1 := NewMetricKey("test", UnitDimensionless, "aaa")
		So(key1.name, ShouldEqual, "test")
		So(key1.description, ShouldEqual, "aaa")
		So(key1.unit, ShouldEqual, UnitDimensionless)

		key2 := NewMetricKey("test2", UnitByte, "")
		So(key2.name, ShouldEqual, "test2")
		So(key2.description, ShouldBeEmpty)
		So(key2.unit, ShouldEqual, UnitByte)
	})
}

func TestGetCounter(t *testing.T) {
	Convey("test GetCounter method", t, func() {
		empty := GetCounter(nil)
		So(empty, ShouldEqual, emptyCount)

		testCounter := GetCounter(NewMetricKey("test-counter", UnitByte, ""))

		Convey("test Incr", func() {
			testCounter.IncrInt(1)
			testCounter.IncrFloat(1.0)
		})

		Convey("test Async", func() {
			testCounter.Async(func(ctx stdCtx.Context, counter ICounter) {
				counter.IncrInt(1)
			})
		})
	})

}

func TestGetGauge(t *testing.T) {
	Convey("test GetGauge method", t, func() {
		empty := GetGauge(NewMetricKey("", UnitMillisecond, ""))
		So(empty, ShouldEqual, emptyGauge)

		testGauge := GetGauge(NewMetricKey("test-gauge", UnitMillisecond, ""))

		Convey("test Incr", func() {
			testGauge.IncrInt(1)
			testGauge.IncrFloat(1.0)
			testGauge.IncrInt(-10)
			testGauge.IncrFloat(-10)
		})

		Convey("test Async", func() {
			testGauge.Async(func(ctx stdCtx.Context, gauge IGauge) {
				gauge.IncrInt(2)
				gauge.IncrFloat(3)
			})
		})
	})
}

func TestGetHistogram(t *testing.T) {
	Convey("test GetCounter method", t, func() {
		empty := GetHistogram(NewMetricKey("test-histogram", "", ""))
		So(empty, ShouldEqual, emptyHistogram)

		testHistogram := GetHistogram(NewMetricKey("test-histogram", UnitDimensionless, ""))

		Convey("test Incr", func() {
			testHistogram.RecordInt(1)
			testHistogram.RecordFloat(1.0)
		})

		Convey("test Async", func() {
			testHistogram.Async(func(ctx stdCtx.Context, his IHistogram) {
				his.RecordFloat(1)
			})
		})
	})
}
