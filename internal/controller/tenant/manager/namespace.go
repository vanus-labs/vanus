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

package manager

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

type NamespaceManager interface {
	Init(ctx context.Context) error
	AddNamespace(ctx context.Context, ns *metadata.Namespace) error
	DeleteNamespace(ctx context.Context, id vanus.ID) error
	GetNamespace(ctx context.Context, id vanus.ID) *metadata.Namespace
	GetNamespaceByName(ctx context.Context, name string) *metadata.Namespace
	ListNamespace(ctx context.Context) []*metadata.Namespace
}

var _ NamespaceManager = &namespaceManager{}

type namespaceManager struct {
	lock       sync.RWMutex
	namespaces map[vanus.ID]*metadata.Namespace
	kvClient   kv.Client
}

func NewNamespaceManager(client kv.Client) NamespaceManager {
	return &namespaceManager{
		kvClient:   client,
		namespaces: map[vanus.ID]*metadata.Namespace{},
	}
}

func (m *namespaceManager) Init(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	pairs, err := m.kvClient.List(ctx, kv.NamespaceAllKey())
	if err != nil {
		return err
	}
	m.namespaces = make(map[vanus.ID]*metadata.Namespace, len(pairs))
	for _, pair := range pairs {
		var ns metadata.Namespace
		err = json.Unmarshal(pair.Value, &ns)
		if err != nil {
			return err
		}
		m.namespaces[ns.ID] = &ns
	}
	return nil
}

func (m *namespaceManager) AddNamespace(ctx context.Context, ns *metadata.Namespace) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, err := json.Marshal(ns)
	if err != nil {
		return err
	}
	err = m.kvClient.Set(ctx, kv.NamespaceKey(ns.ID), v)
	if err != nil {
		return err
	}
	m.namespaces[ns.ID] = ns
	return nil
}

func (m *namespaceManager) DeleteNamespace(ctx context.Context, id vanus.ID) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.namespaces[id]
	if !exist {
		return nil
	}
	err := m.kvClient.Delete(ctx, kv.NamespaceKey(id))
	if err != nil {
		return err
	}
	delete(m.namespaces, id)
	return nil
}

func (m *namespaceManager) GetNamespace(ctx context.Context, id vanus.ID) *metadata.Namespace {
	m.lock.RLock()
	defer m.lock.RUnlock()
	ns, exist := m.namespaces[id]
	if !exist {
		return nil
	}
	return ns
}

func (m *namespaceManager) GetNamespaceByName(ctx context.Context, name string) *metadata.Namespace {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for i := range m.namespaces {
		if m.namespaces[i].Name == name {
			return m.namespaces[i]
		}
	}
	return nil
}

func (m *namespaceManager) ListNamespace(ctx context.Context) []*metadata.Namespace {
	m.lock.RLock()
	defer m.lock.RUnlock()
	list := make([]*metadata.Namespace, len(m.namespaces))
	for i := range m.namespaces {
		list[i] = m.namespaces[i]
	}
	return list
}
