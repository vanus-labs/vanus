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
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCeSQLFilter_Filter(t *testing.T) {
	tests := map[string]testAttributeValue{
		"NO expression": {value: "", expect: NoFilter},
		"No Match":      {value: fmt.Sprintf("type = '%s'", "nomatch type"), expect: FailFilter},
		"Match":         {value: fmt.Sprintf("type = '%s'", eventType), expect: PassFilter},
	}
	for name, tc := range tests {
		convey.Convey(name, t, func() {
			convey.So(tc.expect, convey.ShouldEqual, NewCESQLFilter(tc.value).Filter(makeEvent()))
		})
	}
}
