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
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	meta "github.com/linkall-labs/vsproto/pkg/meta"
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
			Logs:      Convert2ProtoEventLog(eb.EventLogs...),
		}
	}
	return pebs
}

type Eventlog struct {
	// global unique id
	ID         vanus.ID `json:"id"`
	EventbusID vanus.ID `json:"eventbus_id"`
}

func Convert2ProtoEventLog(ins ...*Eventlog) []*meta.EventLog {
	pels := make([]*meta.EventLog, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eli := ins[idx]
		pels[idx] = &meta.EventLog{
			//EventBusName:  eli.EventBusName,
			EventLogId:    eli.ID.Uint64(),
			ServerAddress: "127.0.0.1:2048",
		}
	}
	return pels
}

type VolumeMetadata struct {
	ID           vanus.ID          `json:"id"`
	Capacity     int64             `json:"capacity"`
	Used         int64             `json:"used"`
	BlockNumbers int               `json:"block_numbers"`
	Blocks       map[string]string `json:"blocks"`
}

type Block struct {
	ID        vanus.ID `json:"id"`
	Capacity  int64    `json:"capacity"`
	Size      int64    `json:"size"`
	VolumeID  vanus.ID `json:"volume_id"`
	SegmentID vanus.ID `json:"event_log_id"`
}
