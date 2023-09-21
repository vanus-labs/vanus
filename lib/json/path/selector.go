// Copyright 2023 Linkall Inc.
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

package path

type Selector interface {
	Apply()
}

type nameSelector struct {
	member string
}

// Make sure nameSelector implements Selector.
var _ Selector = (*nameSelector)(nil)

func (ns *nameSelector) Apply() {}

type wildcardSelector struct{}

// Make sure wildcardSelector implements Selector.
var _ Selector = (*wildcardSelector)(nil)

func (ws *wildcardSelector) Apply() {}

type indexSelector struct {
	index int
}

// Make sure indexSelector implements Selector.
var _ Selector = (*indexSelector)(nil)

func (is *indexSelector) Apply() {}

type arraySliceSelector struct {
	start *int
	end   *int
	step  int
}

// Make sure arraySliceSelector implements Selector.
var _ Selector = (*arraySliceSelector)(nil)

func (ass *arraySliceSelector) Apply() {}
