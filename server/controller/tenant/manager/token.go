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

//go:generate mockgen -source=token.go -destination=mock_token.go -package=manager
package manager

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/vanus-labs/vanus/api/errors"
	vanus "github.com/vanus-labs/vanus/api/vsr"
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/server/controller/tenant/metadata"
)

type TokenManager interface {
	Init(ctx context.Context) error
	GetUser(ctx context.Context, token string) (string, error)
	AddToken(ctx context.Context, user *metadata.Token) error
	DeleteToken(ctx context.Context, id vanus.ID) error
	GetToken(ctx context.Context, id vanus.ID) (*metadata.Token, error)
	GetUserToken(ctx context.Context, identifier string) []*metadata.Token
	ListToken(ctx context.Context) []*metadata.Token
}

var _ TokenManager = &tokenManager{}

type tokenManager struct {
	lock       sync.RWMutex
	tokens     map[vanus.ID]*metadata.Token
	users      map[string]map[vanus.ID]struct{}
	tokenUsers map[string]string
	kvClient   kv.Client
}

func NewTokenManager(kvClient kv.Client) TokenManager {
	return &tokenManager{
		tokens:     map[vanus.ID]*metadata.Token{},
		users:      map[string]map[vanus.ID]struct{}{},
		tokenUsers: map[string]string{},
		kvClient:   kvClient,
	}
}

func (m *tokenManager) Init(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	pairs, err := m.kvClient.List(ctx, kv.UserTokenAllKey())
	if err != nil {
		return err
	}
	m.tokens = make(map[vanus.ID]*metadata.Token, len(pairs))
	for _, pair := range pairs {
		var token metadata.Token
		err = json.Unmarshal(pair.Value, &token)
		if err != nil {
			return err
		}
		m.tokens[token.ID] = &token
		m.tokenUsers[token.Token] = token.UserIdentifier
		ids, exist := m.users[token.UserIdentifier]
		if !exist {
			ids = map[vanus.ID]struct{}{}
			m.users[token.UserIdentifier] = ids
		}
		ids[token.ID] = struct{}{}
	}
	return nil
}

func (m *tokenManager) GetUser(_ context.Context, token string) (string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	user, exist := m.tokenUsers[token]
	if !exist {
		return "", errors.ErrResourceNotFound
	}
	return user, nil
}

func (m *tokenManager) AddToken(ctx context.Context, token *metadata.Token) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	v, err := json.Marshal(token)
	if err != nil {
		return err
	}
	err = m.kvClient.Set(ctx, kv.UserTokenKey(token.ID), v)
	if err != nil {
		return err
	}
	m.tokens[token.ID] = token
	m.tokenUsers[token.Token] = token.UserIdentifier
	ids, exist := m.users[token.UserIdentifier]
	if !exist {
		ids = map[vanus.ID]struct{}{}
		m.users[token.UserIdentifier] = ids
	}
	ids[token.ID] = struct{}{}
	return nil
}

func (m *tokenManager) DeleteToken(ctx context.Context, id vanus.ID) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	token, exist := m.tokens[id]
	if !exist {
		return nil
	}
	if token.UserIdentifier == primitive.DefaultUser && len(m.users[token.UserIdentifier]) == 1 {
		return errors.ErrResourceCanNotOp.WithMessage("can't remove default user all token")
	}
	err := m.kvClient.Delete(ctx, kv.UserTokenKey(id))
	if err != nil {
		return err
	}
	delete(m.tokens, id)
	delete(m.tokenUsers, token.Token)
	delete(m.users[token.UserIdentifier], id)
	return nil
}

func (m *tokenManager) GetToken(_ context.Context, id vanus.ID) (*metadata.Token, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	token, exist := m.tokens[id]
	if !exist {
		return nil, errors.ErrResourceNotFound
	}
	return token, nil
}

func (m *tokenManager) GetUserToken(_ context.Context, identifier string) []*metadata.Token {
	m.lock.RLock()
	defer m.lock.RUnlock()
	ids, exist := m.users[identifier]
	if !exist {
		return nil
	}
	list := make([]*metadata.Token, len(ids))
	i := 0
	for id := range ids {
		list[i] = m.tokens[id]
		i++
	}
	return list
}

func (m *tokenManager) ListToken(_ context.Context) []*metadata.Token {
	m.lock.RLock()
	defer m.lock.RUnlock()
	list := make([]*metadata.Token, len(m.tokens))
	i := 0
	for id := range m.tokens {
		list[i] = m.tokens[id]
		i++
	}
	return list
}
