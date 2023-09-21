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
	"sync"
	"time"
)

type waitGroup struct {
	wg   *sync.WaitGroup
	data int64
}

func exponent(tick time.Duration, m, n int64) time.Duration {
	result := int64(1)
	for i := n; i > 0; i >>= 1 {
		if i&1 != 0 {
			result *= m
		}
		m *= m
	}
	return time.Duration(result) * tick
}
