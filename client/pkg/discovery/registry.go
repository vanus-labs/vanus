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

package discovery

import (
	"sync"
)

var (
	nameServices = make(map[string]NameService)
	registryMu   = sync.RWMutex{}
)

func Register(scheme string, ns NameService) {
	registryMu.Lock()
	defer registryMu.Unlock()
	nameServices[scheme] = ns
}

func Unregister(scheme string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(nameServices, scheme)
}

func Find(scheme string) NameService {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return nameServices[scheme]
}
