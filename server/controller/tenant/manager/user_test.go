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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/server/controller/tenant/metadata"
)

func makeUser(user string) *metadata.User {
	return &metadata.User{
		Identifier: user,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
}

func TestUserManager_Init(t *testing.T) {
	Convey("user init", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserManager(kvClient).(*userManager)
		md1 := makeUser("test1")
		md2 := makeUser("test2")
		b1, _ := json.Marshal(md1)
		b2, _ := json.Marshal(md2)
		kvClient.EXPECT().List(gomock.Any(), gomock.Eq(kv.UserAllKey())).Return(
			[]kv.Pair{
				{
					Key:   kv.UserKey(md1.Identifier),
					Value: b1,
				},
				{
					Key:   kv.UserKey(md2.Identifier),
					Value: b2,
				},
			}, nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
		So(len(m.users), ShouldEqual, 2)
		_, exist := m.users[md1.Identifier]
		So(exist, ShouldBeTrue)
		_, exist = m.users[md2.Identifier]
		So(exist, ShouldBeTrue)
	})
}

func TestMockUserManager_AddUser(t *testing.T) {
	Convey("user add", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserManager(kvClient).(*userManager)
		md := makeUser("test1")
		kvClient.EXPECT().Set(gomock.Any(), gomock.Eq(kv.UserKey(md.Identifier)), gomock.Any()).Return(nil)
		err := m.AddUser(ctx, md)
		So(err, ShouldBeNil)
		So(len(m.users), ShouldEqual, 1)
		_, exist := m.users[md.Identifier]
		So(exist, ShouldBeTrue)
	})
}

func TestMockUserManager_DeleteUser(t *testing.T) {
	Convey("user delete", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserManager(kvClient).(*userManager)
		md := makeUser("test1")
		Convey("user no exist", func() {
			err := m.DeleteUser(ctx, md.Identifier)
			So(err, ShouldBeNil)
		})
		Convey("user default", func() {
			md = makeUser(primitive.DefaultUser)
			m.users[md.Identifier] = md
			err := m.DeleteUser(ctx, md.Identifier)
			So(err, ShouldNotBeNil)
		})
		Convey("user exist", func() {
			m.users[md.Identifier] = md
			kvClient.EXPECT().Delete(gomock.Any(), gomock.Eq(kv.UserKey(md.Identifier))).Return(nil)
			err := m.DeleteUser(ctx, md.Identifier)
			So(err, ShouldBeNil)
			So(len(m.users), ShouldEqual, 0)
			_, exist := m.users[md.Identifier]
			So(exist, ShouldBeFalse)
		})
	})
}

func TestMockUserManager_GetUser(t *testing.T) {
	Convey("user get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserManager(kvClient).(*userManager)
		md := makeUser("test1")
		Convey("User no exist", func() {
			get := m.GetUser(ctx, md.Identifier)
			So(get, ShouldBeNil)
		})
		Convey("User exist", func() {
			m.users[md.Identifier] = md
			get := m.GetUser(ctx, md.Identifier)
			So(get, ShouldNotBeNil)
		})
	})
}

func TestMockUserManager_ListUser(t *testing.T) {
	Convey("User get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewUserManager(kvClient).(*userManager)
		md1 := makeUser("test1")
		md2 := makeUser("test2")
		list := m.ListUser(ctx)
		So(len(list), ShouldEqual, 0)
		m.users[md1.Identifier] = md1
		m.users[md2.Identifier] = md2
		list = m.ListUser(ctx)
		So(len(list), ShouldEqual, 2)
	})
}
