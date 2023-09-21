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

//go:generate mockgen -source=user.go -destination=mock_user.go -package=manager
package manager

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/vanus-labs/vanus/api/errors"
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/server/controller/tenant/metadata"
)

type UserManager interface {
	Init(ctx context.Context) error
	GetUser(ctx context.Context, identifier string) *metadata.User
	AddUser(ctx context.Context, user *metadata.User) error
	DeleteUser(ctx context.Context, identifier string) error
	ListUser(ctx context.Context) []*metadata.User
}

var _ UserManager = &userManager{}

type userManager struct {
	lock     sync.RWMutex
	users    map[string]*metadata.User
	kvClient kv.Client
}

func NewUserManager(kvClient kv.Client) UserManager {
	return &userManager{
		users:    map[string]*metadata.User{},
		kvClient: kvClient,
	}
}

func (m *userManager) Init(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	pairs, err := m.kvClient.List(ctx, kv.UserAllKey())
	if err != nil {
		return err
	}
	m.users = make(map[string]*metadata.User, len(pairs))
	for _, pair := range pairs {
		var user metadata.User
		err = json.Unmarshal(pair.Value, &user)
		if err != nil {
			return err
		}
		m.users[user.Identifier] = &user
	}
	return nil
}

func (m *userManager) ListUser(_ context.Context) []*metadata.User {
	m.lock.RLock()
	defer m.lock.RUnlock()
	list := make([]*metadata.User, len(m.users))
	i := 0
	for _, user := range m.users {
		list[i] = user
		i++
	}
	return list
}

func (m *userManager) GetUser(_ context.Context, identifier string) *metadata.User {
	m.lock.RLock()
	defer m.lock.RUnlock()
	user, exist := m.users[identifier]
	if !exist {
		return nil
	}
	return user
}

func (m *userManager) AddUser(ctx context.Context, user *metadata.User) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.users[user.Identifier]
	if exist {
		return errors.ErrResourceAlreadyExist.WithMessage("user exist")
	}
	v, err := json.Marshal(user)
	if err != nil {
		return err
	}
	err = m.kvClient.Set(ctx, kv.UserKey(user.Identifier), v)
	if err != nil {
		return err
	}
	m.users[user.Identifier] = user
	return nil
}

func (m *userManager) DeleteUser(ctx context.Context, identifier string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, exist := m.users[identifier]
	if !exist {
		return nil
	}
	if identifier == primitive.DefaultUser {
		return errors.ErrResourceCanNotOp.WithMessage("can't remove default user")
	}
	err := m.kvClient.Delete(ctx, kv.UserKey(identifier))
	if err != nil {
		return err
	}
	delete(m.users, identifier)
	return nil
}
