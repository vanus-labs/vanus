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

package worker

import (
	"context"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
)

type Manager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	AddSubscription(ctx context.Context, sub *primitive.Subscription) error
	RemoveSubscription(ctx context.Context, ID vanus.ID) error
	PauseSubscription(ctx context.Context, ID vanus.ID) error
}

type manager struct {
	subscriptions sync.Map
	lock          sync.RWMutex
	offsetManager *offset.Manager
	ctx           context.Context
	stop          context.CancelFunc
	config        Config
}

func NewTriggerManager(config Config) Manager {
	if config.CleanSubscriptionTimeout == 0 {
		config.CleanSubscriptionTimeout = 5 * time.Second
	}
	m := &manager{
		config:        config,
		offsetManager: offset.NewOffsetManager(),
	}
	m.ctx, m.stop = context.WithCancel(context.Background())
	return m
}

func (m *manager) Start(ctx context.Context) error {
	return nil
}

func (m *manager) Stop(ctx context.Context) error {
	return nil
}

func (m *manager) AddSubscription(ctx context.Context, sub *primitive.Subscription) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	return nil
}

func (m *manager) RemoveSubscription(ctx context.Context, ID vanus.ID) error {
	return nil
}

func (m *manager) PauseSubscription(ctx context.Context, ID vanus.ID) error {
	return nil
}
