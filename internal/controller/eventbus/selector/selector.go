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

package selector

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
)

type SegmentServerSelector interface {
	Select(context.Context, int64) *info.SegmentServerInfo
}

type segmentServerRoundRobinSelector struct {
	count int64
	m     *map[string]*info.SegmentServerInfo
}

func NewSegmentServerRoundRobinSelector(m *map[string]*info.SegmentServerInfo) SegmentServerSelector {
	return &segmentServerRoundRobinSelector{
		count: 0,
		m:     m,
	}
}

func (s *segmentServerRoundRobinSelector) Select(ctx context.Context, size int64) *info.SegmentServerInfo {
	// TODO optimize
	keys := make([]string, 0)
	for k := range *s.m {
		keys = append(keys, k)
	}
	ssi := (*s.m)[keys[s.count%int64(len(keys))]]
	s.count += 1
	return ssi
}
