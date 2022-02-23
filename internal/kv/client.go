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

package kv

import "time"

type Client interface {
	Get(key string) ([]byte, error)
	Create(key string, value []byte) error
	Set(key string, value []byte) error
	Update(key string, value []byte) error
	Exists(key string) (bool, error)
	SetWithTTL(key string, value []byte, ttl time.Duration) error
	Delete(key string) error
	DeleteDir(path string) error
	List(path string) ([]Pair, error)
	Watch(key string, stopCh <-chan struct{}) (chan Pair, chan error)
	WatchTree(path string, stopCh <-chan struct{}) (chan Pair, chan error)
	CompareAndSwap(key string, preValue, value []byte) error
	CompareAndDelete(key string, preValue []byte) error
}

type Pair struct {
	Key   string
	Value []byte
}
