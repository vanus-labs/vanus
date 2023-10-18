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

	primitive "github.com/vanus-labs/vanus/pkg"
)

const (
	defaultBufferSize      = 1 << 10
	defaultDeliveryTimeout = 5 * time.Second
	defaultMaxWriteAttempt = 3
	defaultGoroutineSize   = 10000
	defaultMaxUACKNumber   = 10000
	defaultBatchSize       = 32
)

type Config struct {
	BufferSize        int
	MaxRetryAttempts  int32
	DeliveryTimeout   time.Duration
	RateLimit         uint32
	Controllers       []string
	MaxWriteAttempt   int
	Ordered           bool
	DisableDeadLetter bool

	GoroutineSize int
	SendBatchSize int
	PullBatchSize int
	MaxUACKNumber int
	Proxy         *Proxy
}

type Proxy struct {
	Address          string `yaml:"address"`
	TargetHeaderName string `yaml:"target_header_name"`
}

func defaultConfig() Config {
	c := Config{
		BufferSize:       defaultBufferSize,
		MaxRetryAttempts: primitive.MaxRetryAttempts,
		DeliveryTimeout:  defaultDeliveryTimeout,
		MaxWriteAttempt:  defaultMaxWriteAttempt,
		GoroutineSize:    defaultGoroutineSize,
		SendBatchSize:    defaultBatchSize,
		MaxUACKNumber:    defaultMaxUACKNumber,
		PullBatchSize:    defaultBatchSize,
	}
	return c
}

type Option func(t *trigger)

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
		if attempts < 0 {
			attempts = primitive.MaxRetryAttempts
		}
		t.config.MaxRetryAttempts = attempts
	}
}

func WithDeliveryTimeout(timeout uint32) Option {
	return func(t *trigger) {
		if timeout == 0 {
			t.config.DeliveryTimeout = defaultDeliveryTimeout
			return
		}
		t.config.DeliveryTimeout = time.Duration(timeout) * time.Millisecond
	}
}

func WithOrdered(ordered bool) Option {
	return func(t *trigger) {
		t.config.Ordered = ordered
	}
}

func WithRateLimit(rateLimit uint32) Option {
	return func(t *trigger) {
		t.config.RateLimit = rateLimit
		if rateLimit == 0 {
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

func WithDisableDeadLetter(disable bool) Option {
	return func(t *trigger) {
		t.config.DisableDeadLetter = disable
	}
}

func WithGoroutineSize(size int) Option {
	return func(t *trigger) {
		if size <= 0 {
			return
		}
		t.config.GoroutineSize = size
	}
}

func WithSendBatchSize(batchSize int) Option {
	return func(t *trigger) {
		if batchSize <= 0 {
			return
		}
		t.config.SendBatchSize = batchSize
	}
}

func WithPullBatchSize(batchSize int) Option {
	return func(t *trigger) {
		if batchSize <= 0 {
			return
		}
		t.config.PullBatchSize = batchSize
	}
}

func WithMaxUACKNumber(maxUACKNumber int) Option {
	return func(t *trigger) {
		if maxUACKNumber <= 0 {
			return
		}
		t.config.MaxUACKNumber = maxUACKNumber
	}
}

func WithProxy(proxy *Proxy) Option {
	return func(t *trigger) {
		t.config.Proxy = proxy
	}
}
