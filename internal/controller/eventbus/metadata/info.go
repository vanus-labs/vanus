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

package metadata

import (
	"encoding/json"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vsproto/pkg/meta"
)

type Eventbus struct {
	ID        vanus.ID    `json:"id"`
	Name      string      `json:"name"`
	LogNumber int         `json:"log_number"`
	EventLogs []*Eventlog `json:"event_logs"`
}

func Convert2ProtoEventBus(ins ...*Eventbus) []*meta.EventBus {
	pebs := make([]*meta.EventBus, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eb := ins[idx]
		pebs[idx] = &meta.EventBus{
			Name:      eb.Name,
			LogNumber: int32(eb.LogNumber),
		}
	}
	return pebs
}

type Eventlog struct {
	// global unique id
	ID         vanus.ID `json:"id"`
	EventbusID vanus.ID `json:"eventbus_id"`
}

type VolumeMetadata struct {
	ID       vanus.ID          `json:"id"`
	Capacity int64             `json:"capacity"`
	Used     int64             `json:"used"`
	Blocks   map[uint64]*Block `json:"blocks"`
}

type Block struct {
	ID         vanus.ID `json:"id"`
	Capacity   int64    `json:"capacity"`
	Size       int64    `json:"size"`
	VolumeID   vanus.ID `json:"volume_id"`
	EventlogID vanus.ID `json:"eventlog_id"`
	SegmentID  vanus.ID `json:"segment_id"`
}

func (bl *Block) String() string {
	data, _ := json.Marshal(bl)
	return string(data)
}
