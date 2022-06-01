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

package eventbus

import (
	// standard library
	"context"
	"sort"
	"strings"
	"sync"

	// this project
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
)

func UseNameService(scheme string) {
	ns := newNameService()
	discovery.Register(scheme, ns)
}

func newNameService() *nameService {
	ns := &nameService{
		impls: make(map[string]*nameServiceImpl),
	}
	return ns
}

type nameService struct {
	impls map[string]*nameServiceImpl
	mu    sync.RWMutex
}

// make sure nameService implements discovery.NameService.
var _ discovery.NameService = (*nameService)(nil)

func (ns *nameService) LookupWritableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	impl, err := ns.selectNameServiceImpl(eventbus.Endpoints)
	if err != nil {
		return nil, err
	}
	return impl.LookupWritableLogs(ctx, eventbus)
}

func (ns *nameService) LookupReadableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	impl, err := ns.selectNameServiceImpl(eventbus.Endpoints)
	if err != nil {
		return nil, err
	}
	return impl.LookupReadableLogs(ctx, eventbus)
}

func (ns *nameService) selectNameServiceImpl(endpoints []string) (*nameServiceImpl, error) {
	sort.Strings(endpoints)
	key := strings.Join(endpoints, ",")
	impl := func() *nameServiceImpl {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return ns.impls[key]
	}()
	if impl == nil {
		// TODO: to optimize lock, but newNameServiceImpl does not block now.
		ns.mu.Lock()
		defer ns.mu.Unlock()
		impl = ns.impls[key]
		if impl == nil { // double check
			var err error
			impl, err = newNameServiceImpl(endpoints)
			if err != nil {
				return nil, err
			}
			ns.impls[key] = impl
		}
	}
	return impl, nil
}
