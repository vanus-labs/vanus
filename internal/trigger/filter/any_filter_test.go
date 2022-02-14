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

func TestAnyFilter_Filter(t *testing.T) {
	tests := map[string]testStruct{
		"Empty": {filter: NewAnyFilter(), expect: NoFilter},
		"Pass":  {filter: NewAnyFilter(&passFilter{}), expect: PassFilter},
		"Fail":  {filter: NewAnyFilter(&failFilter{}), expect: FailFilter},
	}
	for name, tc := range tests {
		convey.Convey(name, t, func() {
			convey.So(tc.expect, convey.ShouldEqual, tc.filter.Filter(makeEvent()))
		})
	}
}
