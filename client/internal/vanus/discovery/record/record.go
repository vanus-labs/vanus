// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

type LogSegment struct {
	ID          uint64
	StartOffset int64
	EndOffset   int64
	Writable    bool

	Blocks        map[uint64]*SegmentBlock
	LeaderBlockID uint64
}

type SegmentBlock struct {
	ID       uint64
	Endpoint string
}

func (s *LogSegment) GetLeaderEndpoint() string {
	return s.Blocks[s.LeaderBlockID].Endpoint
}
