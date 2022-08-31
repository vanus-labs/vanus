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

	"github.com/linkall-labs/vanus/internal/primitive"

	"go.uber.org/ratelimit"
)

const (
	defaultBufferSize        = 1 << 10
	defaultFilterProcessSize = 2
	defaultDeliveryTimeout   = 5 * time.Second
)

type Config struct {
	FilterProcessSize  int
	BufferSize         int
	MaxRetryAttempts   int32
	DeliveryTimeout    time.Duration
	RateLimit          int32
	Controllers        []string
	DeadLetterEventbus string
}

func defaultConfig() Config {
	c := Config{
		FilterProcessSize:  defaultFilterProcessSize,
		BufferSize:         defaultBufferSize,
		MaxRetryAttempts:   primitive.MaxRetryAttempts,
		DeliveryTimeout:    defaultDeliveryTimeout,
		DeadLetterEventbus: primitive.DeadLetterEventbusName,
	}
	return c
}

type Option func(t *trigger)

func WithFilterProcessSize(size int) Option {
	return func(t *trigger) {
		if size <= 0 {
			return
		}
		t.config.FilterProcessSize = size
	}
}

func WithBufferSize(size int) Option {
	return func(t *trigger) {
		if size <= 0 {
			return
		}
		t.config.BufferSize = size
	}
}

func WithMaxRetryAttempts(attempts int32) Option {
	return func(t *trigger) {
		if attempts <= 0 {
			attempts = primitive.MaxRetryAttempts
		}
		t.config.MaxRetryAttempts = attempts
	}
}

func WithDeliveryTimeout(timeout int32) Option {
	return func(t *trigger) {
		if timeout <= 0 {
			t.config.DeliveryTimeout = defaultDeliveryTimeout
			return
		}
		t.config.DeliveryTimeout = time.Duration(timeout) * time.Millisecond
	}
}

func WithRateLimit(rateLimit int32) Option {
	return func(t *trigger) {
		t.config.RateLimit = rateLimit
		if rateLimit <= 0 {
			t.rateLimiter = ratelimit.NewUnlimited()
			return
		}
		t.rateLimiter = ratelimit.New(int(rateLimit))
	}
}

func WithControllers(controllers []string) Option {
	return func(t *trigger) {
		t.config.Controllers = controllers
	}
}

func WithDeadLetterEventbus(eventbus string) Option {
	return func(t *trigger) {
		if eventbus == "" {
			eventbus = primitive.DeadLetterEventbusName
		}
		t.config.DeadLetterEventbus = eventbus
	}
}
