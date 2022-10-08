// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"sync"

	eb "github.com/linkall-labs/vanus/client/internal/vanus/eventbus"

	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	"github.com/linkall-labs/vanus/observability/tracing"
)

const (
	XVanusLogOffset = eventbus.XVanusLogOffset
)

type Client interface {
	Eventbus(ctx context.Context, ebName string) eventbus.Eventbus
}

type client struct {
	// Endpoints is a list of URLs.
	Endpoints  []string
	eventbuses map[string]eventbus.Eventbus

	mu     sync.RWMutex
	tracer *tracing.Tracer
}

func (c *client) Eventbus(ctx context.Context, ebName string) eventbus.Eventbus {
	_, span := c.tracer.Start(ctx, "Eventbus")
	defer span.End()

	bus := func() eventbus.Eventbus {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if bus, ok := c.eventbuses[ebName]; ok {
			return bus
		} else {
			return nil
		}
	}()

	if bus == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		var ok bool
		if bus, ok = c.eventbuses[ebName]; !ok { // double check
			cfg := &eb.Config{
				Endpoints: c.Endpoints,
				Name:      ebName,
			}
			bus = eventbus.NewEventBus(cfg)
			c.eventbuses[cfg.Name] = bus
		}
	}

	return bus
}

func Connect(endpoints []string) Client {
	return &client{
		Endpoints:  endpoints,
		eventbuses: make(map[string]eventbus.Eventbus, 0),
	}
}
