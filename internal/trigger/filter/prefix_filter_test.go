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

package filter

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPrefixFilter_Filter(t *testing.T) {
	tests := map[string]testAttributeValue{
		"No attribute":    {attribute: "test-attribute", value: "prefix", expect: FailFilter},
		"No Match prefix": {attribute: "source", value: "no match prefix", expect: FailFilter},
		"Match prefix":    {attribute: "source", value: eventSource, expect: PassFilter},
	}
	for name, tc := range tests {
		convey.Convey(name, t, func() {
			convey.So(tc.expect, convey.ShouldEqual, NewPrefixFilter(tc.attribute, tc.value).Filter(makeEvent()))
		})
	}
}
