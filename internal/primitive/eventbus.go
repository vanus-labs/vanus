// Copyright 2023 Linkall Inc.
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

package primitive

import "github.com/vanus-labs/vanus/internal/primitive/vanus"

func GetDeadLetterEventbusName(eventbusName string) string {
	return SystemEventbusNamePrefix + "dl_" + eventbusName
}

func GetRetryEventbusName(eventbusName string) string {
	return SystemEventbusNamePrefix + "retry_eb"
}

func GetDeadLetterEventbusID(eventbusID vanus.ID) uint64 {
	return 0
}

func GetRetryEventbusID(eventbusID vanus.ID) uint64 {
	return 0
}
