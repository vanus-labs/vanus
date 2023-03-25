// Copyright 2023 Linkall Inc.
//
// LiceuserRoleed under the Apache LiceuserRolee, Version 2.0 (the "LiceuserRolee");
// you may not use this file except in compliance with the LiceuserRolee.
// You may obtain a copy of the LiceuserRolee at
//
//     http://www.apache.org/liceuserRolees/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the LiceuserRolee is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the LiceuserRolee for the specific language governing permissiouserRole and
// limitatiouserRole under the LiceuserRolee.

package manager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/errors"
)

func addUserRoleForManger(m *userRoleManager, userRole *metadata.UserRole) {
	roles, exist := m.userRoles[userRole.UserIdentifier]
	if !exist {
		roles = map[string]*metadata.UserRole{}
		m.userRoles[userRole.UserIdentifier] = roles
	}
	roles[userRole.GetRoleID()] = userRole
	resource := m.resourceRole[userRole.ResourceID]
	resource = append(resource, userRole)
	m.resourceRole[userRole.ResourceID] = resource
}

func makeUserRole(user string) *metadata.UserRole {
	return &metadata.UserRole{
		UserIdentifier: user,
		ResourceKind:   authorization.ResourceNamespace,
		ResourceID:     vanus.NewTestID(),
		Role:           authorization.RoleAdmin,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func TestUserRoleManager_Init(t *testing.T) {
	Convey("userRole init", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserRoleManager(kvClient).(*userRoleManager)
		userRole1 := makeUserRole("user")
		userRole2 := makeUserRole("user")
		userRole3 := makeUserRole("user2")
		b1, _ := json.Marshal(userRole1)
		b2, _ := json.Marshal(userRole2)
		b3, _ := json.Marshal(userRole3)
		kvClient.EXPECT().List(gomock.Any(), gomock.Eq(kv.UserRoleAllKey())).Return(
			[]kv.Pair{
				{
					Key:   m.getKVKey(userRole1),
					Value: b1,
				},
				{
					Key:   m.getKVKey(userRole2),
					Value: b2,
				},
				{
					Key:   m.getKVKey(userRole3),
					Value: b3,
				},
			}, nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
		So(len(m.userRoles), ShouldEqual, 2)
		So(len(m.resourceRole), ShouldEqual, 3)
		userRoles, exist := m.userRoles[userRole1.UserIdentifier]
		So(exist, ShouldBeTrue)
		So(len(userRoles), ShouldEqual, 2)
		userRole, exist := userRoles[userRole1.GetRoleID()]
		So(exist, ShouldBeTrue)
		So(userRole.ResourceID, ShouldEqual, userRole1.ResourceID)
		userRole, exist = userRoles[userRole2.GetRoleID()]
		So(exist, ShouldBeTrue)
		So(userRole.ResourceID, ShouldEqual, userRole2.ResourceID)
		userRoles, exist = m.userRoles[userRole3.UserIdentifier]
		So(exist, ShouldBeTrue)
		So(len(userRoles), ShouldEqual, 1)
	})
}

func TestMockUserRoleManager_AddUserRole(t *testing.T) {
	Convey("userRole add", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserRoleManager(kvClient).(*userRoleManager)
		userRole := makeUserRole("user")
		kvClient.EXPECT().Set(gomock.Any(), gomock.Eq(m.getKVKey(userRole)), gomock.Any()).Return(nil)
		err := m.AddUserRole(ctx, userRole)
		So(err, ShouldBeNil)
		So(len(m.userRoles), ShouldEqual, 1)
		So(len(m.resourceRole), ShouldEqual, 1)
		role, exist := m.userRoles[userRole.UserIdentifier]
		So(exist, ShouldBeTrue)
		_, exist = role[userRole.GetRoleID()]
		So(exist, ShouldBeTrue)
		_, exist = m.resourceRole[userRole.ResourceID]
		So(exist, ShouldBeTrue)
	})
}

func TestMockUserRoleManager_DeleteUserRole(t *testing.T) {
	Convey("userRole delete", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserRoleManager(kvClient).(*userRoleManager)
		userRole := makeUserRole("user")
		Convey("userRole no exist", func() {
			err := m.DeleteUserRole(ctx, userRole)
			So(err, ShouldBeNil)
		})
		Convey("userRole default", func() {
			userRole = makeUserRole(primitive.DefaultUser)
			addUserRoleForManger(m, userRole)
			err := m.DeleteUserRole(ctx, userRole)
			So(err, ShouldNotBeNil)
		})
		Convey("userRole exist", func() {
			kvClient.EXPECT().Delete(gomock.Any(), gomock.Eq(m.getKVKey(userRole))).AnyTimes().Return(nil)
			addUserRoleForManger(m, userRole)
			Convey("clean role", func() {
				err := m.DeleteUserRole(ctx, userRole)
				So(err, ShouldBeNil)
				So(len(m.userRoles), ShouldEqual, 0)
				_, exist := m.userRoles[userRole.UserIdentifier]
				So(exist, ShouldBeFalse)
				_, exist = m.resourceRole[userRole.ResourceID]
				So(exist, ShouldBeFalse)
			})
			Convey("delete a role for user has two role", func() {
				userRole2 := makeUserRole("user")
				addUserRoleForManger(m, userRole2)
				err := m.DeleteUserRole(ctx, userRole)
				So(err, ShouldBeNil)
				So(len(m.userRoles), ShouldEqual, 1)
				_, exist := m.userRoles[userRole.UserIdentifier]
				So(exist, ShouldBeTrue)
				_, exist = m.resourceRole[userRole.ResourceID]
				So(exist, ShouldBeFalse)
				_, exist = m.resourceRole[userRole2.ResourceID]
				So(exist, ShouldBeTrue)
			})
			Convey("delete a role one resource for two user", func() {
				userRole2 := makeUserRole("user2")
				userRole2.ResourceID = userRole.ResourceID
				addUserRoleForManger(m, userRole2)
				err := m.DeleteUserRole(ctx, userRole)
				So(err, ShouldBeNil)
				So(len(m.userRoles), ShouldEqual, 1)
				_, exist := m.userRoles[userRole.UserIdentifier]
				So(exist, ShouldBeFalse)
				_, exist = m.resourceRole[userRole.ResourceID]
				So(exist, ShouldBeTrue)
				_, exist = m.userRoles[userRole2.UserIdentifier]
				So(exist, ShouldBeTrue)
			})
		})
	})
}

func TestMockUserRoleManager_UserRoleExist(t *testing.T) {
	Convey("userRole exist", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserRoleManager(kvClient).(*userRoleManager)
		userRole := makeUserRole("user")
		Convey("userRole no exist", func() {
			exist := m.IsUserRoleExist(ctx, userRole)
			So(exist, ShouldBeFalse)
		})
		Convey("userRole exist", func() {
			addUserRoleForManger(m, userRole)
			exist := m.IsUserRoleExist(ctx, userRole)
			So(exist, ShouldBeTrue)
		})
	})
}

func TestMockUserRoleManager_GetUserRole(t *testing.T) {
	Convey("userRole get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserRoleManager(kvClient).(*userRoleManager)
		userRole := makeUserRole("user")
		Convey("userRole no exist", func() {
			_, err := m.GetUserRoleByUser(ctx, userRole.UserIdentifier)
			So(err, ShouldNotBeNil)
			So(errors.Is(err, errors.ErrResourceNotFound), ShouldBeTrue)
		})
		Convey("userRole exist", func() {
			addUserRoleForManger(m, userRole)
			userRoles, err := m.GetUserRoleByUser(ctx, userRole.UserIdentifier)
			So(err, ShouldBeNil)
			So(len(userRoles), ShouldEqual, 1)
		})
	})
}
