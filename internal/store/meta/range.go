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

import "github.com/huandu/skiplist"

type RangeCallback func(key []byte, value interface{}) error

type Ranger interface {
	Range(cb RangeCallback) error
}

type skiplistRange struct {
	l *skiplist.SkipList
}

var _ Ranger = (*skiplistRange)(nil)

func SkiplistRange(l *skiplist.SkipList) Ranger {
	return &skiplistRange{
		l: l,
	}
}

func (r *skiplistRange) Range(cb RangeCallback) error {
	for el := r.l.Front(); el != nil; el = el.Next() {
		key, ok := el.Key().([]byte)
		if !ok {
			panic("codec: key is not []byte")
		}
		if err := cb(key, el.Value); err != nil {
			return err
		}
	}
	return nil
}

type kvRange struct {
	key   []byte
	value interface{}
}

var _ Ranger = (*kvRange)(nil)

func KVRange(key []byte, value interface{}) Ranger {
	return &kvRange{
		key:   key,
		value: value,
	}
}

func (r *kvRange) Range(cb RangeCallback) error {
	return cb(r.key, r.value)
}
