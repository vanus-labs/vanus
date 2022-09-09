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

package segment

import (
	stdCtx "context"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/pkg/util"
	. "github.com/smartystreets/goconvey/convey"
)

func TestPollingManager_All(t *testing.T) {
	Convey("test polling manager", t, func() {
		blkID := vanus.NewID()
		pm := &pollingMgr{}
		ctx := stdCtx.Background()
		ch := pm.Add(ctx, blkID)
		So(ch, ShouldBeNil)

		tCtx, cancel := stdCtx.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		ch = pm.Add(tCtx, blkID)
		So(ch, ShouldNotBeNil)
		So(util.MapLen(&pm.blockPollingMap), ShouldEqual, 1)

		tCtx, cancel = stdCtx.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		ch2 := pm.Add(tCtx, blkID)
		So(ch, ShouldEqual, ch2)

		pm.NewMessageArrived(vanus.NewID())
		pm.NewMessageArrived(blkID)
		_, ok := <-ch
		So(ok, ShouldBeFalse)

		tCtx, cancel = stdCtx.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		ch3 := pm.Add(tCtx, blkID)
		So(ch, ShouldNotEqual, ch3)

		time.Sleep(60 * time.Millisecond)
		ch4 := pm.Add(tCtx, blkID)
		So(ch4, ShouldBeNil)

		pm.Destroy()
		So(util.MapLen(&pm.blockPollingMap), ShouldEqual, 0)
		_, ok = <-ch3
		So(ok, ShouldBeFalse)
	})
}
