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

package authorization

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/cluster"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func TestAuthorization_Authorize(t *testing.T) {
	Convey("authorize", t, func() {
		ctrl := gomock.NewController(t)
		roleClient := NewMockRoleClient(ctrl)
		clusterCli := cluster.NewMockCluster(ctrl)
		ctx := context.Background()
		m := NewAuthorization(roleClient, clusterCli).(*authorization)
		user := "user"
		Convey("cluster admin", func() {
			roleClient.EXPECT().IsClusterAdmin(ctx, gomock.Eq(user)).Return(true, nil)
			result, err := m.Authorize(ctx, user, &defaultAttributes{})
			So(err, ShouldBeNil)
			So(result, ShouldBeTrue)
		})
		roleClient.EXPECT().IsClusterAdmin(ctx, gomock.Any()).AnyTimes().Return(false, nil)
		Convey("namespace admin", func() {
			id := vanus.NewTestID()
			nsID := vanus.NewTestID()
			roleClient.EXPECT().GetUserRole(ctx, gomock.Eq(user)).AnyTimes().Return([]*UserRole{
				{
					UserIdentifier: user,
					ResourceKind:   ResourceNamespace,
					ResourceID:     nsID,
					Role:           RoleAdmin,
					BuiltIn:        true,
				},
			}, nil)
			Convey("op namespace has role", func() {
				result, err := m.Authorize(ctx, user, &defaultAttributes{
					resourceKind: ResourceNamespace,
					resourceID:   nsID,
					action:       NamespaceGrant,
				})
				So(err, ShouldBeNil)
				So(result, ShouldBeTrue)
			})
			Convey("op eventbus has role", func() {
				ebCli := cluster.NewMockEventbusService(ctrl)
				clusterCli.EXPECT().EventbusService().Return(ebCli)
				ebCli.EXPECT().GetEventbus(ctx, gomock.Eq(id.Uint64())).Return(&metapb.Eventbus{
					Id:          id.Uint64(),
					NamespaceId: nsID.Uint64(),
				}, nil)
				result, err := m.Authorize(ctx, user, &defaultAttributes{
					resourceKind: ResourceEventbus,
					resourceID:   id,
					action:       EventbusGrant,
				})
				So(err, ShouldBeNil)
				So(result, ShouldBeTrue)
			})
			Convey("op subscription has role", func() {
				triggerCli := cluster.NewMockTriggerService(ctrl)
				clusterCli.EXPECT().TriggerService().Return(triggerCli)
				triggerCli.EXPECT().GetSubscription(ctx, gomock.Eq(id.Uint64())).Return(&metapb.Subscription{
					Id:          id.Uint64(),
					NamespaceId: nsID.Uint64(),
				}, nil)
				result, err := m.Authorize(ctx, user, &defaultAttributes{
					resourceKind: ResourceSubscription,
					resourceID:   id,
					action:       SubscriptionGrant,
				})
				So(err, ShouldBeNil)
				So(result, ShouldBeTrue)
			})
			Convey("op cluster no role", func() {
				result, err := m.Authorize(ctx, user, &defaultAttributes{action: UserCreate})
				So(err, ShouldBeNil)
				So(result, ShouldBeFalse)
			})
		})
	})
}
