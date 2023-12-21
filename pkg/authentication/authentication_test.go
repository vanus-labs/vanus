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

package authentication

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/mock/gomock"

	"github.com/vanus-labs/vanus/api/errors"
)

func TestAuthentication_Authenticate(t *testing.T) {
	Convey("authenticate", t, func() {
		ctrl := gomock.NewController(t)
		tokenClient := NewMockTokenClient(ctrl)
		ctx := context.Background()
		m := NewAuthentication(tokenClient).(*authentication)
		token := "test"
		Convey("cache exist", func() {
			m.tokens.Store(token, "user")
			user, err := m.Authenticate(ctx, token)
			So(err, ShouldBeNil)
			So(user, ShouldEqual, "user")
		})
		Convey("cache no exist", func() {
			Convey("user exist", func() {
				tokenClient.EXPECT().GetUser(gomock.Any(), gomock.Eq(token)).Return("user", nil)
				user, err := m.Authenticate(ctx, token)
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "user")
			})
			Convey("user no exist", func() {
				tokenClient.EXPECT().GetUser(gomock.Any(), gomock.Eq(token)).Return("", nil)
				_, err := m.Authenticate(ctx, token)
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errors.ErrResourceNotFound), ShouldBeTrue)
			})
		})
	})
}
