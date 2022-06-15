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

package filter_test

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
)

func filterBenchmark(f filter.Filter, event ce.Event) func(b *testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.Filter(event)
		}
	}
}

func BenchmarkFilter(b *testing.B) {
	event := cetest.FullEvent()
	b.Run("noFilter", filterBenchmark(filter.NewNoFilter(), event))
	b.Run("exact", filterBenchmark(filter.NewExactFilter(map[string]string{"type": event.Type()}), event))
	b.Run("not", filterBenchmark(filter.NewNotFilter(filter.NewExactFilter(map[string]string{"type": event.Type()})), event))
	b.Run("suffix", filterBenchmark(filter.NewSuffixFilter(map[string]string{"type": event.Type()}), event))
	b.Run("prefix", filterBenchmark(filter.NewPrefixFilter(map[string]string{"type": event.Type()}), event))
	b.Run("ceSQL", filterBenchmark(filter.NewCESQLFilter("source = 'testSource'"), event))
	event.SetData(ce.ApplicationJSON, map[string]interface{}{
		"key": "value",
		"num": 10,
	})
	b.Run("cel", filterBenchmark(filter.NewCELFilter("$key.(string) == 'value'"), event))
}
