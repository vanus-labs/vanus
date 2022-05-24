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

package eventlog

import (
	stdCtx "context"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestEventlogManager_Run(t *testing.T) {
	Convey("", t, func() {

	})
}

func TestEventlogManager_Stop(t *testing.T) {

}

func TestEventlogManager_CreateEventLog(t *testing.T) {
	Convey("test AcquireEventLog", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		eventbusID := vanus.NewID()
		logMD, err := utMgr.AcquireEventLog(stdCtx.Background(), eventbusID)
		Convey("validate metadata", func() {
			So(err, ShouldBeNil)
			So(logMD.ID, ShouldEqual, eventbusID)
		})

		Convey("validate eventlog", func() {
			elog := utMgr.getEventLog(stdCtx.Background(), logMD.ID)
			So(elog, ShouldNotBeNil)
			So(elog.size(), ShouldEqual, 2)
			So(elog.appendableSegmentNumber(), ShouldEqual, 2)
		})
	})
}

func TestEventlogManager_GetEventLogSegmentList(t *testing.T) {

}

func TestEventlogManager_GetAppendableSegment(t *testing.T) {

}

func TestEventlogManager_UpdateSegment(t *testing.T) {

}

func TestEventlogManager_GetSegmentByBlockID(t *testing.T) {

}

func TestEventlogManager_GetBlock(t *testing.T) {

}

func TestEventlogManager_GetSegment(t *testing.T) {

}

func TestEventlogManager_UpdateSegmentReplicas(t *testing.T) {

}
