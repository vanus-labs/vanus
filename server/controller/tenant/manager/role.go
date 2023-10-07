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

//go:generate mockgen -source=role.go -destination=mock_role.go -package=manager
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

type UserRoleManager interface {
	Init(ctx context.Context) error
	AddUserRole(ctx context.Context, role *metadata.UserRole) error
	DeleteUserRole(ctx context.Context, role *metadata.UserRole) error
	GetUserRoleByUser(ctx context.Context, userIdentifier string) ([]*metadata.UserRole, error)
	GetUserRoleByResourceID(ctx context.Context, resourceID vanus.ID) ([]*metadata.UserRole, error)
	IsUserRoleExist(ctx context.Context, role *metadata.UserRole) bool
}

var _ UserRoleManager = &userRoleManager{}

type userRoleManager struct {
	lock         sync.RWMutex
	userRoles    map[string]map[string]*metadata.UserRole // key: user,roleID
	resourceRole map[vanus.ID][]*metadata.UserRole        // key: resourceID
	kvClient     kv.Client
}

func NewUserRoleManager(kvClient kv.Client) UserRoleManager {
	return &userRoleManager{
		userRoles:    map[string]map[string]*metadata.UserRole{},
		resourceRole: map[vanus.ID][]*metadata.UserRole{},
		kvClient:     kvClient,
	}
}

func (m *userRoleManager) Init(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	pairs, err := m.kvClient.List(ctx, kv.UserRoleAllKey())
	if err != nil {
		return err
	}
	m.userRoles = make(map[string]map[string]*metadata.UserRole)
	m.resourceRole = make(map[vanus.ID][]*metadata.UserRole)
	for _, pair := range pairs {
		var userRole metadata.UserRole
		err = json.Unmarshal(pair.Value, &userRole)
		if err != nil {
			return err
		}
		roles, exist := m.userRoles[userRole.UserIdentifier]
		if !exist {
			roles = map[string]*metadata.UserRole{}
			m.userRoles[userRole.UserIdentifier] = roles
		}
		roles[userRole.GetRoleID()] = &userRole
		if userRole.BuiltIn() {
			resourceRoles := m.resourceRole[userRole.ResourceID]
			resourceRoles = append(resourceRoles, &userRole)
			m.resourceRole[userRole.ResourceID] = resourceRoles
			continue
		}
		// todo custom role
	}
	return nil
}

func (m *userRoleManager) AddUserRole(ctx context.Context, userRole *metadata.UserRole) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	roles, exist := m.userRoles[userRole.UserIdentifier]
	if !exist {
		roles = map[string]*metadata.UserRole{}
		m.userRoles[userRole.UserIdentifier] = roles
	}
	roleID := userRole.GetRoleID()
	_, exist = roles[roleID]
	if exist {
		return nil
	}
	v, err := json.Marshal(userRole)
	if err != nil {
		return err
	}
	err = m.kvClient.Set(ctx, m.getKVKey(userRole), v)
	if err != nil {
		return err
	}
	roles[roleID] = userRole
	if userRole.BuiltIn() {
		resourceRoles := m.resourceRole[userRole.ResourceID]
		resourceRoles = append(resourceRoles, userRole)
		m.resourceRole[userRole.ResourceID] = resourceRoles
		return nil
	}
	// todo custom role
	return nil
}

func (m *userRoleManager) DeleteUserRole(ctx context.Context, userRole *metadata.UserRole) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	roles, exist := m.userRoles[userRole.UserIdentifier]
	if !exist {
		return nil
	}
	if userRole.UserIdentifier == primitive.DefaultUser {
		return errors.ErrResourceCanNotOp.WithMessage("can't modify default user admin")
	}
	roleID := userRole.GetRoleID()
	_, exist = roles[roleID]
	if !exist {
		return nil
	}
	err := m.kvClient.Delete(ctx, m.getKVKey(userRole))
	if err != nil {
		return err
	}
	delete(roles, roleID)
	if len(roles) == 0 {
		delete(m.userRoles, userRole.UserIdentifier)
	}
	resourceRoles, exist := m.resourceRole[userRole.ResourceID]
	if !exist {
		return nil
	}
	var newRole []*metadata.UserRole
	for i, role := range resourceRoles {
		if role.UserIdentifier != userRole.UserIdentifier || role.GetRoleID() != roleID {
			newRole = append(newRole, resourceRoles[i])
		}
	}
	if len(newRole) == 0 {
		delete(m.resourceRole, userRole.ResourceID)
	} else {
		m.resourceRole[userRole.ResourceID] = newRole
	}
	return nil
}

func (m *userRoleManager) IsUserRoleExist(_ context.Context, userRole *metadata.UserRole) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	roles, exist := m.userRoles[userRole.UserIdentifier]
	if !exist {
		return false
	}
	_, exist = roles[userRole.GetRoleID()]
	return exist
}

func (m *userRoleManager) GetUserRoleByUser(_ context.Context, userIdentifier string) ([]*metadata.UserRole, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	roles, exist := m.userRoles[userIdentifier]
	if !exist {
		return nil, errors.ErrResourceNotFound
	}
	list := make([]*metadata.UserRole, len(roles))
	i := 0
	for id := range roles {
		list[i] = roles[id]
		i++
	}
	return list, nil
}

func (m *userRoleManager) GetUserRoleByResourceID(_ context.Context, resourceID vanus.ID,
) ([]*metadata.UserRole, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	roles, exist := m.resourceRole[resourceID]
	if !exist {
		return nil, errors.ErrResourceNotFound
	}
	return roles, nil
}

func (m *userRoleManager) getKVKey(userRole *metadata.UserRole) string {
	return kv.UserRoleKey(userRole.UserIdentifier, userRole.GetRoleID())
}
