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

package offset

import (
	"sync"
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriptionOffset(t *testing.T) {
	Convey("subscription offset", t, func() {
		eventLogID := vanus.NewID()
		subOffset := NewSubscriptionOffset(vanus.NewID())
		Convey("commit with no receive", func() {
			offsetBegin := uint64(1)
			commitEnd := offsetBegin + 10
			for offset := offsetBegin; offset < commitEnd; offset++ {
				subOffset.EventCommit(info.OffsetInfo{
					EventLogID: eventLogID,
					Offset:     offset,
				})
			}
			commits := subOffset.GetCommit()
			So(0, ShouldEqual, len(commits))
		})
		Convey("commit with receive", func() {
			offsetBegin := uint64(1)
			offsetEnd := uint64(100)
			var wg sync.WaitGroup
			for offset := offsetBegin; offset < offsetEnd; offset++ {
				wg.Add(1)
				go func(offset uint64) {
					defer wg.Done()
					subOffset.EventReceive(info.OffsetInfo{
						EventLogID: eventLogID,
						Offset:     offset,
					})
				}(offset)
			}
			wg.Wait()
			commitEnd := offsetBegin + 10
			for offset := offsetBegin; offset < commitEnd; offset++ {
				wg.Add(1)
				go func(offset uint64) {
					defer wg.Done()
					subOffset.EventCommit(info.OffsetInfo{
						EventLogID: eventLogID,
						Offset:     offset,
					})
				}(offset)
			}
			wg.Wait()
			commits := subOffset.GetCommit()
			So(1, ShouldEqual, len(commits))
			So(commitEnd, ShouldEqual, commits[0].Offset)
			offsetBegin = commitEnd
			commitEnd = offsetEnd
			for offset := offsetBegin; offset <= commitEnd; offset++ {
				wg.Add(1)
				go func(offset uint64) {
					defer wg.Done()
					subOffset.EventCommit(info.OffsetInfo{
						EventLogID: eventLogID,
						Offset:     offset,
					})
				}(offset)
			}
			wg.Wait()
			commits = subOffset.GetCommit()
			So(1, ShouldEqual, len(commits))
			So(offsetEnd, ShouldEqual, commits[0].Offset)
			commits = subOffset.GetCommit()
			So(1, ShouldEqual, len(commits))
			So(offsetEnd, ShouldEqual, commits[0].Offset)
			subOffset.Clear()
			commits = subOffset.GetCommit()
			So(0, ShouldEqual, len(commits))
		})
	})
}

func TestOffsetTracker(t *testing.T) {
	Convey("test offset tracker", t, func() {
		offset := uint64(0)
		tracker := initOffset(offset)
		So(tracker.offsetToCommit(), ShouldEqual, offset)
		tracker.putOffset(offset)
		So(tracker.offsetToCommit(), ShouldEqual, offset)
		tracker.putOffset(offset + 2)
		So(tracker.offsetToCommit(), ShouldEqual, offset)
		tracker.putOffset(offset + 1)
		So(tracker.offsetToCommit(), ShouldEqual, offset)
		tracker.putOffset(offset + 3)
		So(tracker.offsetToCommit(), ShouldEqual, offset)
		tracker.commitOffset(offset)
		So(tracker.offsetToCommit(), ShouldEqual, offset+1)
		tracker.commitOffset(offset + 2)
		So(tracker.offsetToCommit(), ShouldEqual, offset+1)
		tracker.commitOffset(offset + 1)
		So(tracker.offsetToCommit(), ShouldEqual, offset+3)
		tracker.commitOffset(offset + 3)
		So(tracker.offsetToCommit(), ShouldEqual, offset+4)
	})
}
