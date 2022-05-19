// Copyright 2022 Linkall Inc.
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

package block

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewVolumeRoundRobin(t *testing.T) {
	Convey("test round-rubin selector in normal", t, func() {
		ctrl := gomock.NewController(t)
		srv1 := server.NewMockInstance(ctrl)
		srv2 := server.NewMockInstance(ctrl)

		srv1.EXPECT().GetMeta().Return(&metadata.VolumeMetadata{
			ID: vanus.NewIDFromUint64(1),
		}).AnyTimes()
		srv2.EXPECT().GetMeta().Return(&metadata.VolumeMetadata{
			ID: vanus.NewIDFromUint64(2),
		}).AnyTimes()

		srvs := []server.Instance{srv1, srv2}
		selector := NewVolumeRoundRobin(func() []server.Instance {
			return srvs
		})
		Convey("test select", func() {
			instances := selector.Select(1, 64*1024*1024)
			So(instances, ShouldHaveLength, 1)
			So(instances[0].GetMeta().ID.Uint64(), ShouldEqual, uint64(1))

			instances = selector.Select(1, 640*1024*1024)
			So(instances[0].GetMeta().ID.Uint64(), ShouldEqual, uint64(2))
			So(selector.(*volumeRoundRobinSelector).count, ShouldEqual, 2)

			instances = selector.Select(2, 64*1024*1024)
			So(instances, ShouldHaveLength, 2)
			So(instances[0].GetMeta().ID.Uint64(), ShouldEqual, uint64(1))
			So(selector.(*volumeRoundRobinSelector).count, ShouldEqual, 3)

			instances = selector.Select(3, 64*1024*1024)
			So(instances, ShouldHaveLength, 3)

			So(instances[0].GetMeta().ID.Uint64(), ShouldEqual, uint64(2))
			So(instances[1].GetMeta().ID.Uint64(), ShouldEqual, uint64(1))
			So(instances[2].GetMeta().ID.Uint64(), ShouldEqual, uint64(2))
			So(selector.(*volumeRoundRobinSelector).count, ShouldEqual, 4)
		})

		srv1.EXPECT().ID().Return(vanus.NewIDFromUint64(1)).AnyTimes()
		srv2.EXPECT().ID().Return(vanus.NewIDFromUint64(2)).AnyTimes()
		Convey("test select instance by id", func() {
			ins := selector.SelectByID(vanus.NewIDFromUint64(1))
			So(ins, ShouldNotBeNil)
			So(ins.GetMeta().ID.Uint64(), ShouldEqual, uint64(1))

			ins = selector.SelectByID(vanus.NewIDFromUint64(2))
			So(ins, ShouldNotBeNil)
			So(ins.GetMeta().ID.Uint64(), ShouldEqual, uint64(2))

			ins = selector.SelectByID(vanus.NewIDFromUint64(3))
			So(ins, ShouldBeNil)
		})

		Convey("test invalid arguments", func() {
			instances := selector.Select(0, 64*1024*1024)
			So(instances, ShouldNotBeNil)
			So(instances, ShouldHaveLength, 0)

			instances = selector.Select(2, 0)
			So(instances, ShouldNotBeNil)
			So(instances, ShouldHaveLength, 0)
		})
	})
}

func TestNewVolumeRoundRobinAbnormal(t *testing.T) {
	Convey("test nil conditions", t, func() {
		selector := NewVolumeRoundRobin(func() []server.Instance {
			return []server.Instance{}
		})
		instances := selector.Select(3, 64*1024*1024)
		So(instances, ShouldNotBeNil)
		So(instances, ShouldHaveLength, 0)
	})
}
