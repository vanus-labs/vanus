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

package util

import (
	"fmt"
	"testing"
)

func TestBitSet_NextClearBit(t *testing.T) {
	n := 100001
	s := NewBitSet()
	for i := 0; i < n; i++ {
		s.Set(i)
	}
	v := s.NextClearBit(0)
	fmt.Println(v)
	ns := s.Clear(n / 64)
	v = ns.NextClearBit(0)
	fmt.Println(v)
}
