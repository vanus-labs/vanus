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

package etcd

import "time"

type Client interface {
	Get(key string) (string, error)
	Create(key, value string) error
	Set(key, value string) error
	Update(key, value string) error
	Exists(key string) (bool, error)
	SetWithTTL(key, value string, ttl time.Duration) error
	Delete(key string) error
	DeleteDir(path string) error
	List(path string) ([]Pair, error)
	Watch(key string, stopCh <-chan struct{}) (chan Pair, chan error)
	WatchTree(path string, stopCh <-chan struct{}) (chan Pair, chan error)
	CompareAndSwap(key, preValue, value string) error
	CompareAndDelete(key, preValue string) error
}

type Pair struct {
	Key   string
	Value string
}
