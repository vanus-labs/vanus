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
	"fmt"
	"github.com/linkall-labs/vanus/internal/util"
	"time"
)

type TriggerWorkerPhase string

const (
	TriggerWorkerPhasePending    = "pending"
	TriggerWorkerPhaseRunning    = "running"
	TriggerWorkerPhasePaused     = "paused"
	TriggerWorkerPhaseDisconnect = "disconnect"
)

type TriggerWorkerInfo struct {
	Id            string              `json:"-"`
	Addr          string              `json:"addr"`
	Phase         TriggerWorkerPhase  `json:"phase"`
	SubIds        map[string]struct{} `json:"-"`
	ReportSubIds  map[string]struct{} `json:"-"`
	HeartbeatTime time.Time           `json:"-"`
}

func NewTriggerWorkerInfo(addr string) *TriggerWorkerInfo {
	return &TriggerWorkerInfo{
		Addr:          addr,
		Id:            util.GetIdByAddr(addr),
		Phase:         TriggerWorkerPhasePending,
		HeartbeatTime: time.Now(),
		ReportSubIds:  map[string]struct{}{},
	}
}

func (tw *TriggerWorkerInfo) String() string {
	return fmt.Sprintf("addr:%s,phase:%v,subIds:%v", tw.Addr, tw.Phase, tw.SubIds)
}

type SubscriptionHeartbeat struct {
	SubId         string
	HeartbeatTime time.Time
	TriggerWorker string
}
