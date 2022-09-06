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

package primitive

const (
	RetryEventbusName      = "__retry_eb"
	DeadLetterEventbusName = "__dl_eb"
	TimerEventbusName      = "__Timer_RS"

	XVanus               = "xvanus"
	XVanusEventbus       = XVanus + "eventbus"
	XVanusDeliveryTime   = XVanus + "deliverytime"
	XVanusRetryAttempts  = XVanus + "retryattempts"
	XVanusSubscriptionID = XVanus + "subscriptionid"

	LastDeliveryTime  = "lastdeliverytime"
	LastDeliveryError = "lastdeliveryerror"
	DeadLetterReason  = "deadletterreason"

	MaxRetryAttempts = 32
)
