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

package meta

import (
	// third-party libraries.
	"github.com/huandu/skiplist"

	// first-party libraries.
	"github.com/vanus-labs/vanus/lib/bytes"
)

func update(m *skiplist.SkipList, key []byte, value interface{}) {
	if value == DeletedMark {
		m.Remove(key)
		return
	}

	set(m, key, value)
}

func set(m *skiplist.SkipList, key []byte, value interface{}) {
	switch val := value.(type) {
	case []byte:
		// Make a copy to avoid modifying value outside.
		bs := bytes.Clone(val)
		m.Set(key, bs)
	default:
		m.Set(key, value)
	}
}

func rawUpdate(m *skiplist.SkipList, key []byte, value interface{}) {
	if value == DeletedMark {
		m.Remove(key)
	} else {
		m.Set(key, value)
	}
}

func merge(dst, src *skiplist.SkipList) {
	for el := src.Front(); el != nil; el = el.Next() {
		rawUpdate(dst, el.Key().([]byte), el.Value)
	}
}
