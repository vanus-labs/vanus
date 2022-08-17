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

package transport

import (
	"context"
	"time"
)

type BackoffTimer interface {
	OriginalSetting(ctx context.Context)
	SuccessHit(ctx context.Context)
	FailedHit(ctx context.Context)
	CanTry() bool
}

func NewBackoffTimer(initRetryTime time.Duration, maxRertyTime time.Duration) BackoffTimer {
	return &backoffTimer{
		startInterval: initRetryTime.Microseconds(),
		curInterval:   initRetryTime.Microseconds(),
		maxInterval:   maxRertyTime.Microseconds(),
		expire:        0,
	}
}

type backoffTimer struct {
	startInterval int64
	curInterval   int64
	maxInterval   int64
	expire        int64
}

var _ BackoffTimer = (*backoffTimer)(nil)

func (t *backoffTimer) OriginalSetting(ctx context.Context) {
	t.curInterval = t.startInterval
	t.expire = 0
}

func (t *backoffTimer) SuccessHit(ctx context.Context) {
	// when connect or send successfully, call this once
	if !t.CanTry() {
		return
	}
	t.curInterval = t.startInterval
	t.expire = 0
}

func (t *backoffTimer) FailedHit(ctx context.Context) {
	// when connect or send failed, call this once
	if !t.CanTry() {
		return
	}
	t.expire = t.curInterval + time.Now().UnixMicro()
	t.curInterval *= 2
	if t.curInterval > t.maxInterval {
		t.curInterval = t.maxInterval
	}
}

func (t *backoffTimer) CanTry() bool {
	// judge whether retry
	return time.Now().UnixMicro() > t.expire
}
