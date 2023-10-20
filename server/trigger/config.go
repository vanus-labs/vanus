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

package trigger

import (
	// standard libraries.
	"time"

	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/server/trigger/trigger"
)

type Config struct {
	Observability  observability.Config `yaml:"observability"`
	TriggerAddr    string
	Port           int                    `yaml:"port"`
	IP             string                 `yaml:"ip"`
	ControllerAddr []string               `yaml:"controllers"`
	Gateway        *trigger.TargetGateway `yaml:"gateway"`

	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// send event goroutine size
	SendEventGoroutineSize int `yaml:"send_event_goroutine_size"`
	// push event batch size when use grpc
	SendEventBatchSize int `yaml:"send_event_batch_size"`
	// var client read event from segment batch size.
	PullEventBatchSize int `yaml:"pull_event_batch_size"`
	// max uack event number
	MaxUACKEventNumber int `yaml:"max_uack_event_number"`
}
