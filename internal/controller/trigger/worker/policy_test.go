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

package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRandomPolicy(t *testing.T) {
	ctx := context.Background()
	Convey("random policy", t, func() {
		p := &RandomPolicy{}
		tWorkers := getTriggerWorker(10)
		first := p.Acquire(ctx, tWorkers)
		second := p.Acquire(ctx, tWorkers)
		So(first.ID, ShouldNotEqual, "")
		So(second.ID, ShouldNotEqual, "")
	})
}

func TestRoundRobinPolicy(t *testing.T) {
	ctx := context.Background()
	Convey("round robin policy", t, func() {
		p := &RoundRobinPolicy{}
		tWorkers := getTriggerWorker(10)
		for i := 0; i < 100; i++ {
			worker := p.Acquire(ctx, tWorkers)
			So(worker.ID, ShouldEqual, fmt.Sprintf("%d", i%10))
		}
	})
}

func getTriggerWorker(size int) []metadata.TriggerWorkerInfo {
	var list []metadata.TriggerWorkerInfo
	for i := 0; i < size; i++ {
		list = append(list, metadata.TriggerWorkerInfo{
			ID:   fmt.Sprintf("%d", i),
			Addr: fmt.Sprintf("addr%d", i),
		})
	}
	return list
}
