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

	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

func addTokenForManger(m *tokenManager, token *metadata.Token) {
	tokens, exist := m.users[token.UserIdentifier]
	if !exist {
		tokens = map[vanus.ID]struct{}{}
		m.users[token.UserIdentifier] = tokens
	}
	tokens[token.ID] = struct{}{}
	m.tokens[token.ID] = token
	m.tokenUsers[token.Token] = token.UserIdentifier
}

func makeToken(user string) *metadata.Token {
	return &metadata.Token{
		UserIdentifier: user,
		Token:          vanus.NewTestID().String(),
		ID:             vanus.NewTestID(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
}

func TestTokenManager_Init(t *testing.T) {
	Convey("token init", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewTokenManager(kvClient).(*tokenManager)
		md1 := makeToken("user1")
		md2 := makeToken("user1")
		md3 := makeToken("user3")
		b1, _ := json.Marshal(md1)
		b2, _ := json.Marshal(md2)
		b3, _ := json.Marshal(md3)
		kvClient.EXPECT().List(gomock.Any(), gomock.Eq(kv.UserTokenAllKey())).Return(
			[]kv.Pair{
				{
					Key:   kv.UserTokenKey(md1.ID),
					Value: b1,
				},
				{
					Key:   kv.UserTokenKey(md2.ID),
					Value: b2,
				},
				{
					Key:   kv.UserTokenKey(md3.ID),
					Value: b3,
				},
			}, nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
		So(len(m.tokens), ShouldEqual, 3)
		So(len(m.users), ShouldEqual, 2)
		So(len(m.tokenUsers), ShouldEqual, 3)
		_, exist := m.tokens[md1.ID]
		So(exist, ShouldBeTrue)
		_, exist = m.tokens[md2.ID]
		So(exist, ShouldBeTrue)
		_, exist = m.tokens[md3.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestMockTokenManager_AddToken(t *testing.T) {
	Convey("token add", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewTokenManager(kvClient).(*tokenManager)
		md := makeToken("user")
		kvClient.EXPECT().Set(gomock.Any(), gomock.Eq(kv.UserTokenKey(md.ID)), gomock.Any()).Return(nil)
		err := m.AddToken(ctx, md)
		So(err, ShouldBeNil)
		So(len(m.tokens), ShouldEqual, 1)
		So(len(m.users), ShouldEqual, 1)
		So(len(m.tokenUsers), ShouldEqual, 1)
		_, exist := m.tokens[md.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestMockTokenManager_DeleteToken(t *testing.T) {
	Convey("token delete", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewTokenManager(kvClient).(*tokenManager)
		md := makeToken("test1")
		Convey("token no exist", func() {
			err := m.DeleteToken(ctx, md.ID)
			So(err, ShouldBeNil)
		})
		Convey("token default", func() {
			md = makeToken(primitive.DefaultUser)
			addTokenForManger(m, md)
			err := m.DeleteToken(ctx, md.ID)
			So(err, ShouldNotBeNil)
		})
		Convey("token exist", func() {
			addTokenForManger(m, md)
			kvClient.EXPECT().Delete(gomock.Any(), gomock.Eq(kv.UserTokenKey(md.ID))).Return(nil)
			err := m.DeleteToken(ctx, md.ID)
			So(err, ShouldBeNil)
			So(len(m.tokens), ShouldEqual, 0)
			_, exist := m.tokens[md.ID]
			So(exist, ShouldBeFalse)
		})
	})
}

func TestMockTokenManager_GetToken(t *testing.T) {
	Convey("token get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewTokenManager(kvClient).(*tokenManager)
		md := makeToken("user")
		Convey("token no exist", func() {
			_, err := m.GetToken(ctx, md.ID)
			So(err, ShouldNotBeNil)
		})
		Convey("token exist", func() {
			addTokenForManger(m, md)
			get, err := m.GetToken(ctx, md.ID)
			So(err, ShouldBeNil)
			So(get, ShouldNotBeNil)
		})
		Convey("get user by token", func() {
			addTokenForManger(m, md)
			user, err := m.GetUser(ctx, md.Token)
			So(err, ShouldBeNil)
			So(user, ShouldNotBeNil)
		})
		Convey("get user token", func() {
			addTokenForManger(m, md)
			get := m.GetUserToken(ctx, md.UserIdentifier)
			So(get, ShouldNotBeNil)
		})
	})
}

func TestMockTokenManager_ListToken(t *testing.T) {
	Convey("token get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewTokenManager(kvClient).(*tokenManager)
		md1 := makeToken("user")
		md2 := makeToken("user")
		list := m.ListToken(ctx)
		So(len(list), ShouldEqual, 0)
		addTokenForManger(m, md1)
		addTokenForManger(m, md2)
		list = m.ListToken(ctx)
		So(len(list), ShouldEqual, 2)
	})
}
