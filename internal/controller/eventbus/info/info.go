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

package info

import (
	"crypto/sha256"
	"fmt"
	meta "github.com/linkall-labs/vsproto/pkg/meta"
	"time"
)

type BusInfo struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	LogNumber int             `json:"log_number"`
	EventLogs []*EventLogInfo `json:"event_logs"`
}

func Convert2ProtoEventBus(ins ...*BusInfo) []*meta.EventBus {
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

type EventLogInfo struct {
	// global unique id
	ID           string `json:"id"`
	EventBusName string `json:"eventbus_name"`
}

func Convert2ProtoEventLog(ins ...*EventLogInfo) []*meta.EventLog {
	pels := make([]*meta.EventLog, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eli := ins[idx]
		pels[idx] = &meta.EventLog{
			EventBusName:  eli.EventBusName,
			EventLogId:    eli.ID,
			ServerAddress: "127.0.0.1:2048",
		}
	}
	return pels
}

type SegmentServerInfo struct {
	id                string
	Address           string
	StartedAt         time.Time
	LastHeartbeatTime time.Time
}

func (in *SegmentServerInfo) ID() string {
	if in.id == "" {
		in.id = fmt.Sprintf("%x", sha256.Sum256([]byte(in.Address)))
	}
	return in.id
}

const (
	volumeAliveInterval = 5 * time.Second
)

//
//func (in *Metadata) IsOnline() bool {
//	return in.assignedSegmentServer != nil && time.Now().Sub(in.assignedSegmentServer.LastHeartbeatTime) < volumeAliveInterval
//}
//
//func (in *Metadata) IsActivity() bool {
//	return in.isActive
//}
//
//func (in *Metadata) Activate(serverInfo *SegmentServerInfo) {
//	in.isActive = true
//	in.assignedSegmentServer = serverInfo
//	//serverInfo.Volume = in
//}
//
//func (in *Metadata) Inactivate() {
//	in.isActive = false
//	in.assignedSegmentServer = nil
//}
//
//func (in *Metadata) GetAccessEndpoint() string {
//	if !in.isActive {
//		return ""
//	}
//	return in.assignedSegmentServer.Address
//}
