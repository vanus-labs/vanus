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

//go:generate mockgen -source=client.go  -destination=mock_client.go -package=client
package client

import (
	// standard libraries.
	"context"
	"sync"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/tracing"

	// this project.
	eb "github.com/linkall-labs/vanus/client/internal/vanus/eventbus"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
)

type Client interface {
	Eventbus(ctx context.Context, ebName string) api.Eventbus
	Disconnect(ctx context.Context)
}

type client struct {
	// Endpoints is a list of URLs.
	Endpoints  []string
	eventbuses map[string]api.Eventbus

	mu     sync.RWMutex
	tracer *tracing.Tracer
}

func (c *client) Eventbus(ctx context.Context, ebName string) api.Eventbus {
	_, span := c.tracer.Start(ctx, "EventbusService")
	defer span.End()

	bus := func() api.Eventbus {
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
			bus = eventbus.NewEventbus(cfg)
			c.eventbuses[cfg.Name] = bus
		}
	}

	return bus
}

func (c *client) Disconnect(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for ebName := range c.eventbuses {
		c.eventbuses[ebName].Close(ctx)
	}
	c.eventbuses = make(map[string]api.Eventbus, 0)
}

func Connect(endpoints []string) Client {
	if len(endpoints) == 0 {
		return nil
	}
	return &client{
		Endpoints:  endpoints,
		eventbuses: make(map[string]api.Eventbus, 0),
	}
}
