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
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
)

type TriggerWorkerPolicy interface {
	Acquire(context.Context, []metadata.TriggerWorkerInfo) metadata.TriggerWorkerInfo
}

type RoundRobinPolicy struct {
	idx uint64
}

func (rr *RoundRobinPolicy) Acquire(ctx context.Context,
	workers []metadata.TriggerWorkerInfo) metadata.TriggerWorkerInfo {
	length := uint64(len(workers))
	idx := atomic.AddUint64(&rr.idx, 1) - 1
	return workers[idx%length]
}

type RandomPolicy struct {
}

func (r *RandomPolicy) Acquire(ctx context.Context, workers []metadata.TriggerWorkerInfo) metadata.TriggerWorkerInfo {
	return workers[rand.New(rand.NewSource(time.Now().Unix())).Intn(len(workers))]
}
