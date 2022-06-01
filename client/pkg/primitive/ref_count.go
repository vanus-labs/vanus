// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package primitive

import "go.uber.org/atomic"

type RefCounter interface {
	Acquire()
	Release() bool
	UseCount() int32
}

type RefCount struct {
	count atomic.Int32
}

func (c *RefCount) Acquire() {
	c.count.Inc()
}

func (c *RefCount) Release() bool {
	return c.count.Dec() == 0
}

func (c *RefCount) UseCount() int32 {
	return c.count.Load()
}
