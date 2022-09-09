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
	// standard libraries.
	"errors"
	"sync"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/tracing"

	// this project.
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

var ErrClosed = errors.New("MetaStore: closed")

type store struct {
	mu sync.RWMutex

	committed *skiplist.SkipList
	version   int64

	wal      *walog.WAL
	snapshot int64

	marshaler Marshaler
	tracer    *tracing.Tracer
}

func (s *store) load(key []byte) (interface{}, bool) {
	return s.committed.GetValue(key)
}

// func (s *store) store(key []byte, value interface{}) {
// 	set(s.committed, key, value)
// }

// func (s *store) delete(key []byte) {
// 	set(s.committed, key, deletedMark)
// }

func set(m *skiplist.SkipList, key []byte, value interface{}) {
	if value == deletedMark {
		m.Remove(key)
	} else {
		m.Set(key, value)
	}
}
