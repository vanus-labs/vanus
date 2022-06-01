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
	"sort"
	"sync"

	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

// VolumeSelector selector for Block creating. The implementation based on different algorithm, typical
// is Round-Robin.
type VolumeSelector interface {
	// Select return #{num} server.Instance as an array, #{size} tell selector how large Block
	// will be created. The same server.Instance maybe placed in different index of returned array
	// in order to make sure that length of returned array equals with #{num}
	Select(num int, size int64) []server.Instance

	// SelectByID return a specified server.Instance with ServerID
	SelectByID(id vanus.ID) server.Instance

	// GetAllVolume get all volumes
	GetAllVolume() []server.Instance
}

// NewVolumeRoundRobin an implementation of round-robin algorithm. Which need a callback function
// for getting all server.Instance in the current cluster.
func NewVolumeRoundRobin(f func() []server.Instance) VolumeSelector {
	return &volumeRoundRobinSelector{
		count:      0,
		getVolumes: f,
	}
}

type volumeRoundRobinSelector struct {
	count      int64
	getVolumes func() []server.Instance
	mutex      sync.Mutex
}

// Select get #{num} instances. each invoking will increase volumeRoundRobinSelector.count ONLY 1
// whatever #{num} is. Because the Segment usually has three replicas, one leader and two follower,
// we should guarantee each segmentServer has a consistent number of Block leader.
//
// round-rubin is a naive algorithm, so that it can't guarantee completely balancing in the cluster, it
// just does best effort of it. There is another advanced algorithm implementation such as
// runtime-statistic-based-algorithm in the future.
func (s *volumeRoundRobinSelector) Select(num int, size int64) []server.Instance {
	instances := make([]server.Instance, 0)
	if num == 0 || size == 0 {
		return instances
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()

	volumes := s.getVolumes()
	if len(volumes) == 0 {
		return instances
	}
	keys := make([]string, 0)
	m := make(map[string]server.Instance)
	for _, v := range volumes {
		keys = append(keys, v.GetMeta().ID.Key())
		m[v.GetMeta().ID.Key()] = v
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	for idx := 0; idx < num; idx++ {
		instances = append(instances, m[keys[(s.count+int64(idx))%int64(len(keys))]])
	}
	s.count++
	return instances
}

func (s *volumeRoundRobinSelector) SelectByID(id vanus.ID) server.Instance {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	volumes := s.getVolumes()
	for idx := range volumes {
		if volumes[idx].ID() == id {
			return volumes[idx]
		}
	}
	return nil
}

func (s *volumeRoundRobinSelector) GetAllVolume() []server.Instance {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.getVolumes()
}
