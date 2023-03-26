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

package metadata

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/errors"
)

func TestUserRole_Validate(t *testing.T) {
	Convey("user role validate", t, func() {
		userRole := UserRole{}
		Convey("identifier is empty", func() {
			err := userRole.Validate()
			So(err, ShouldNotBeNil)
			So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
		})
		userRole.UserIdentifier = "user"
		Convey("built in", func() {
			Convey("role invalid", func() {
				userRole.Role = "test"
				err := userRole.Validate()
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
			})
			Convey("cluster admin", func() {
				userRole.Role = authorization.RoleClusterAdmin
				Convey("valid", func() {
					err := userRole.Validate()
					So(err, ShouldBeNil)
				})
				Convey("resourceKind is not empty", func() {
					userRole.ResourceKind = authorization.ResourceNamespace
					err := userRole.Validate()
					So(err, ShouldNotBeNil)
					So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
				})
				Convey("resourceID is not empty", func() {
					userRole.ResourceID = vanus.NewTestID()
					err := userRole.Validate()
					So(err, ShouldNotBeNil)
					So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
				})
			})
			Convey("other role", func() {
				userRole.Role = authorization.RoleAdmin
				Convey("resourceKind is invalid", func() {
					userRole.ResourceKind = authorization.ResourceUnknown
					err := userRole.Validate()
					So(err, ShouldNotBeNil)
					So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
				})
				userRole.ResourceKind = authorization.ResourceNamespace
				Convey("resourceID is empty", func() {
					err := userRole.Validate()
					So(err, ShouldNotBeNil)
					So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
				})
				userRole.ResourceID = vanus.NewTestID()
				Convey("valid", func() {
					err := userRole.Validate()
					So(err, ShouldBeNil)
				})
				Convey("eventbus role", func() {
					userRole.Role = authorization.RoleRead
					Convey("role invalid", func() {
						err := userRole.Validate()
						So(err, ShouldNotBeNil)
						So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
					})
					userRole.ResourceKind = authorization.ResourceEventbus
					Convey("valid", func() {
						err := userRole.Validate()
						So(err, ShouldBeNil)
					})
				})
			})
		})
		Convey("custom define role", func() {
			userRole.RoleID = "test"
			Convey("valid", func() {
				err := userRole.Validate()
				So(err, ShouldBeNil)
			})
			Convey("role not empty", func() {
				userRole.Role = authorization.RoleAdmin
				err := userRole.Validate()
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
			})
			Convey("resourceKind not empty", func() {
				userRole.ResourceKind = authorization.ResourceNamespace
				err := userRole.Validate()
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
			})
			Convey("resourceID not empty", func() {
				userRole.ResourceID = vanus.NewTestID()
				err := userRole.Validate()
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errors.ErrInvalidRequest), ShouldBeTrue)
			})
		})
	})
}
