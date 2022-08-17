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
	"fmt"
	"time"

	"github.com/linkall-labs/vanus/internal/util"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type TriggerWorkerPhase string

const (
	TriggerWorkerPhasePending    TriggerWorkerPhase = "pending"
	TriggerWorkerPhaseRunning    TriggerWorkerPhase = "running"
	TriggerWorkerPhasePaused     TriggerWorkerPhase = "paused"
	TriggerWorkerPhaseDisconnect TriggerWorkerPhase = "disconnect"
)

type TriggerWorkerInfo struct {
	ID    string             `json:"-"`
	Addr  string             `json:"addr"`
	Phase TriggerWorkerPhase `json:"phase"`
}

func NewTriggerWorkerInfo(addr string) *TriggerWorkerInfo {
	twInfo := &TriggerWorkerInfo{
		Addr:  addr,
		ID:    util.GetIDByAddr(addr),
		Phase: TriggerWorkerPhasePending,
	}
	return twInfo
}

func (tw *TriggerWorkerInfo) String() string {
	return fmt.Sprintf("addr:%s,phase:%v", tw.Addr, tw.Phase)
}

type SubscriptionPhase string

const (
	SubscriptionPhaseCreated   = "created"
	SubscriptionPhasePending   = "pending"
	SubscriptionPhaseScheduled = "scheduled"
	SubscriptionPhaseRunning   = "running"
	SubscriptionPhaseToDelete  = "toDelete"
)

type Subscription struct {
	ID                 vanus.ID                        `json:"id"`
	Source             string                          `json:"source,omitempty"`
	Types              []string                        `json:"types,omitempty"`
	Config             primitive.SubscriptionConfig    `json:"config,omitempty"`
	Filters            []*primitive.SubscriptionFilter `json:"filters,omitempty"`
	Sink               primitive.URI                   `json:"sink,omitempty"`
	SinkCredentialType *primitive.CredentialType       `json:"sink_credential_type,omitempty"`
	SinkCredential     *primitive.SinkCredential       `json:"-"`
	Protocol           primitive.Protocol              `json:"protocol,omitempty"`
	ProtocolSetting    *primitive.ProtocolSetting      `json:"protocol_settings,omitempty"`
	EventBus           string                          `json:"eventbus"`
	Transformer        *primitive.Transformer          `json:"transformer,omitempty"`

	// not from api
	Phase         SubscriptionPhase `json:"phase"`
	TriggerWorker string            `json:"trigger_worker,omitempty"`
	HeartbeatTime time.Time         `json:"-"`
}

// CloneNotFromAPI property not change from api .
func (update *Subscription) CloneNotFromAPI(curr *Subscription) {
	update.ID = curr.ID
	update.TriggerWorker = curr.TriggerWorker
	update.Phase = curr.Phase
	update.HeartbeatTime = curr.HeartbeatTime
}
