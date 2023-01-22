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

package runtime

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

func actionBenchmark(command []interface{}) func(b *testing.B) {
	a, err := NewAction(command)
	if err != nil {
		panic(err)
	}
	event := cetest.FullEvent()
	event.SetExtension("testKey", "testValue")
	event.SetData(ce.ApplicationJSON, map[string]interface{}{"str": "string", "number": 123.456})
	ceCtx := newEventContext(event)
	err = a.Execute(ceCtx)
	if err != nil {
		panic(err)
	}
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ceCtx = newEventContext(event)
			_ = a.Execute(ceCtx)
		}
	}
}

func newEventContext(event ce.Event) *context.EventContext {
	return &context.EventContext{
		Event: &event,
		Define: map[string]interface{}{
			"str":    "defineStr",
			"number": 123.321,
		},
		Data: map[string]interface{}{
			"str":    "dataStr",
			"number": 456.654,
			"second": float64(1669120748),
			"time":   "2022-11-22T20:40:30Z",
		},
	}
}

func BenchmarkAction(b *testing.B) {
	b.Run("delete", actionBenchmark([]interface{}{"delete", "$.data.str"}))
	b.Run("create", actionBenchmark([]interface{}{"create", "$.data.create", "create"}))
	b.Run("replace", actionBenchmark([]interface{}{"replace", "$.data.str", "replaceValue"}))
	b.Run("move", actionBenchmark([]interface{}{"move", "$.data.str", "$.data.strObj.str"}))
	b.Run("rename", actionBenchmark([]interface{}{"rename", "$.data.str", "$.data.strNew"}))
	b.Run("math_add", actionBenchmark([]interface{}{"math_add", "$.data.math_add", "$.data.number", "<number>"}))
	b.Run("math_sub", actionBenchmark([]interface{}{"math_sub", "$.data.math_sub", "$.data.number", "<number>"}))
	b.Run("math_mul", actionBenchmark([]interface{}{"math_mul", "$.data.math_mul", "$.data.number", "<number>"}))
	b.Run("math_div", actionBenchmark([]interface{}{"math_div", "$.data.math_div", "$.data.number", "<number>"}))
	b.Run("unix_time_format", actionBenchmark([]interface{}{"unix_time_format", "$.data.second", "Y-m-d H:i:s"}))
	b.Run("date_format", actionBenchmark([]interface{}{"date_format", "$.data.time", "Y-m-d H:i:s"}))
	b.Run("join", actionBenchmark([]interface{}{"join", "$.data.join", ",", "$.data.str", "<str>"}))
	b.Run("upper_case", actionBenchmark([]interface{}{"upper_case", "$.data.str"}))
	b.Run("lower_case", actionBenchmark([]interface{}{"lower_case", "$.data.str"}))
	b.Run("add_prefix", actionBenchmark([]interface{}{"add_prefix", "$.data.str", "prefix"}))
	b.Run("add_suffix", actionBenchmark([]interface{}{"add_suffix", "$.data.str", "suffix"}))
	b.Run("replace_with_regex", actionBenchmark([]interface{}{"replace_with_regex", "$.data.str", "a", "Aa"}))
	b.Run("capitalize_sentence", actionBenchmark([]interface{}{"capitalize_sentence", "$.data.str"}))
}
