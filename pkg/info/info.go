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
	"encoding/json"

	vanus "github.com/vanus-labs/vanus/api/vsr"
)

type SubscriptionInfo struct {
	SubscriptionID vanus.ID       `json:"subscription_id"`
	Offsets        ListOffsetInfo `json:"offsets"`
}

type OffsetInfo struct {
	EventlogID vanus.ID `json:"eventlog_id"`
	Offset     uint64   `json:"offset"`
}

func (i *OffsetInfo) String() string {
	v, _ := json.Marshal(i)
	return string(v)
}

type ListOffsetInfo []OffsetInfo

func (o ListOffsetInfo) String() string {
	if len(o) == 0 {
		return ""
	}
	v, _ := json.Marshal(o)
	return string(v)
}
