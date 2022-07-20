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
	"time"

	"go.uber.org/ratelimit"
)

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
	RateLimit         int
}

func defaultConfig() Config {
	c := Config{
		FilterProcessSize: defaultFilterProcessSize,
		SendProcessSize:   defaultSendProcessSize,
		BufferSize:        defaultBufferSize,
		MaxRetryTimes:     defaultMaxRetryTimes,
		RetryPeriod:       defaultRetryPeriod,
		SendTimeOut:       defaultSendTimeout,
	}
	return c
}

type Option func(t *Trigger)

func WithFilterProcessSize(size int) Option {
	return func(t *Trigger) {
		if size <= 0 {
			return
		}
		t.config.FilterProcessSize = size
	}
}

func WithSendProcessSize(size int) Option {
	return func(t *Trigger) {
		if size <= 0 {
			return
		}
		t.config.SendProcessSize = size
	}
}

func WithBufferSize(size int) Option {
	return func(t *Trigger) {
		if size <= 0 {
			return
		}
		t.config.BufferSize = size
	}
}

func WithMaxRetryTimes(times int) Option {
	return func(t *Trigger) {
		if times <= 0 {
			return
		}
		t.config.MaxRetryTimes = times
	}
}

func WithRetryPeriod(period time.Duration) Option {
	return func(t *Trigger) {
		if period <= 0 {
			return
		}
		t.config.RetryPeriod = period
	}
}

func WithSendTimeOut(timeout time.Duration) Option {
	return func(t *Trigger) {
		if timeout <= 0 {
			return
		}
		t.config.SendTimeOut = timeout
	}
}

func WithRateLimit(rateLimit int) Option {
	return func(t *Trigger) {
		if rateLimit <= 0 {
			return
		}
		t.config.RateLimit = rateLimit
		t.rateLimiter = ratelimit.New(rateLimit)
	}
}
