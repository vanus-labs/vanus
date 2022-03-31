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

import "time"

const (
	defaultBufferSize        = 1 << 10
	defaultFilterProcessSize = 2
	defaultSendProcessSize   = 2
	defaultMaxRetryTimes     = 3
	defaultRetryPeriod       = 3 * time.Second
	defaultSendTimeout       = 5 * time.Second
)

type Config struct {
	FilterProcessSize int
	SendProcessSize   int
	BufferSize        int
	MaxRetryTimes     int
	RetryPeriod       time.Duration
	SendTimeOut       time.Duration
}

func (c *Config) initConfig() {
	if c.FilterProcessSize <= 0 {
		c.FilterProcessSize = defaultFilterProcessSize
	}
	if c.SendProcessSize <= 0 {
		c.SendProcessSize = defaultSendProcessSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaultBufferSize
	}
	if c.MaxRetryTimes <= 0 {
		c.MaxRetryTimes = defaultMaxRetryTimes
	}
	if c.RetryPeriod <= 0 {
		c.RetryPeriod = defaultRetryPeriod
	}
	if c.SendTimeOut <= 0 {
		c.SendTimeOut = defaultSendTimeout
	}
}
