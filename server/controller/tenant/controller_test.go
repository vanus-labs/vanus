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

package tenant

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/pkg/snowflake"
	"github.com/vanus-labs/vanus/server/controller/tenant/manager"
	"github.com/vanus-labs/vanus/server/controller/tenant/metadata"
)

func TestController_CreateUser(t *testing.T) {
	Convey("user create", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		userManger := manager.NewMockUserManager(mockCtrl)
		ctrl.userManager = userManger
		ctx := context.Background()
		userManger.EXPECT().AddUser(gomock.Any(), gomock.Any()).Return(nil)
		user := "user"
		u, err := ctrl.CreateUser(ctx, &ctrlpb.CreateUserRequest{Identifier: user})
		So(err, ShouldBeNil)
		So(u.Identifier, ShouldEqual, user)
	})
}

func TestController_CreateToken(t *testing.T) {
	Convey("create token", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		userManger := manager.NewMockUserManager(mockCtrl)
		tokenManger := manager.NewMockTokenManager(mockCtrl)
		ctrl.userManager = userManger
		ctrl.tokenManager = tokenManger
		ctx := context.Background()
		snowflake.InitializeFake()
		user := "user"
		userManger.EXPECT().GetUser(gomock.Any(), gomock.Any()).Return(&metadata.User{Identifier: user})
		tokenManger.EXPECT().AddToken(gomock.Any(), gomock.Any()).Return(nil)
		token, err := ctrl.CreateToken(ctx, &ctrlpb.CreateTokenRequest{UserIdentifier: user})
		So(err, ShouldBeNil)
		So(token.UserIdentifier, ShouldEqual, user)
	})
}
