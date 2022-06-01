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

package eventlog

import (
	"sync"
)

type NewOperation func(cfg *Config) (EventLog, error)

var (
	newOps     = make(map[string]NewOperation)
	registryMu = sync.RWMutex{}
)

func Register(scheme string, factory NewOperation) {
	registryMu.Lock()
	defer registryMu.Unlock()
	newOps[scheme] = factory
}

func Unregister(scheme string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(newOps, scheme)
}

func Find(scheme string) NewOperation {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return newOps[scheme]
}
