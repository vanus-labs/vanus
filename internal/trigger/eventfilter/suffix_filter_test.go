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

package eventfilter

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSuffixFilter_Filter(t *testing.T) {
	tests := map[string]testAttributeValue{
		"No attribute":    {attribute: "test-attribute", value: "no.suffix", expect: FailFilter},
		"No Match prefix": {attribute: "source", value: "no.match.suffix", expect: FailFilter},
		"Match prefix":    {attribute: "source", value: eventSource, expect: PassFilter},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			e := makeEvent()
			f, err := NewSuffixFilter(tc.attribute, tc.value)
			if err != nil {
				t.Errorf("error new suffix filter %v", err)
			} else {
				assert.Equal(t, tc.expect, f.Filter(context.TODO(), e))
			}
		})
	}
}
