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

package metrics

const (
	LabelType     = "type"
	LabelVolume   = "volume"
	LabelEventbus = "eventbus"
	LabelEventlog = "eventlog"

	LabelTriggerWorker = "trigger_worker"
	LabelTrigger       = "trigger"
	LabelResult        = "result"
	LabelBlock         = "block"

	LabelTimer = "timer"
)

const (
	LabelValueResourceDynamicCreate        = "dynamic"
	LabelValueResourceManualCreate         = "manual"
	LabelValuePushEventSuccess             = "success"
	LabelValuePushEventFail                = "fail"
	LabelSegmentDeletedBecauseExpired      = "segment_expired"
	LabelSegmentDeletedBecauseCreateFailed = "segment_create_failed"
	LabelSegmentDeletedBecauseDeleted      = "segment_deleted"
)

const (
	LabelScheduledEventDelayTime        = "scheduled_event_delay_time"
	LabelTimerPushScheduledEventTime    = "push_scheduled_event_time"
	LabelTimerDeliverScheduledEventTime = "deliver_scheduled_event_time"
)
