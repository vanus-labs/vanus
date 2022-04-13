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

package allocator

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"sort"
)

type VolumeSelector interface {
	Select(context.Context, int, int64) []volume.Instance
}

type volumeRoundRobinSelector struct {
	count      int64
	getVolumes func() []volume.Instance
}

func NewVolumeRoundRobin(f func() []volume.Instance) VolumeSelector {
	return &volumeRoundRobinSelector{
		count:      0,
		getVolumes: f,
	}
}

func (s *volumeRoundRobinSelector) Select(ctx context.Context, num int, size int64) []volume.Instance {
	instances := make([]volume.Instance, num)
	if num == 0 || size == 0 {
		return instances
	}

	volumes := s.getVolumes()
	if len(volumes) == 0 {
		return nil
	}
	keys := make([]string, 0)
	m := make(map[string]volume.Instance)
	for _, v := range volumes {
		keys = append(keys, v.GetMeta().ID)
		m[v.GetMeta().ID] = v
	}
	sort.Strings(keys)
	for idx := 0; idx < num; idx++ {
		instances[idx] = m[keys[(s.count+int64(idx))%int64(len(keys))]]
	}
	return instances
}
