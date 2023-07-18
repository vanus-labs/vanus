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

//go:generate mockgen -source=client.go -destination=mock_client.go -package=client
package client

import (
	// standard libraries.
	"context"
	"errors"
	"sync"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/tracing"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"

	// this project.
	eb "github.com/vanus-labs/vanus/client/internal/vanus/eventbus"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventbus"
)

type Client interface {
	Eventbus(ctx context.Context, opts ...api.EventbusOption) api.Eventbus
	Disconnect(ctx context.Context)
}

type client struct {
	// Endpoints is a list of URLs.
	Endpoints     []string
	eventbusCache sync.Map

	mu     sync.RWMutex
	tracer *tracing.Tracer
}

func (c *client) Eventbus(ctx context.Context, opts ...api.EventbusOption) api.Eventbus {
	_, span := c.tracer.Start(ctx, "EventbusService")
	defer span.End()

	defaultOpts := api.DefaultEventbusOptions()
	for _, apply := range opts {
		apply(defaultOpts)
	}

	err := GetEventbusIDIfNotSet(ctx, c.Endpoints, defaultOpts)
	if err != nil {
		log.Error(ctx, "get eventbus id failed", map[string]interface{}{
			log.KeyError:    err,
			"eventbus_name": defaultOpts.Name,
			"eventbus_id":   defaultOpts.ID,
		})
		return nil
	}

	bus := func() api.Eventbus {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if value, ok := c.eventbusCache.Load(defaultOpts.ID); ok {
			return value.(api.Eventbus)
		} else {
			return nil
		}
	}()

	if bus == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if value, ok := c.eventbusCache.Load(defaultOpts.ID); ok { // double check
			return value.(api.Eventbus)
		} else {
			cfg := &eb.Config{
				Endpoints: c.Endpoints,
				ID:        defaultOpts.ID,
			}
			newEventbus := eventbus.NewEventbus(cfg)
			c.eventbusCache.Store(defaultOpts.ID, newEventbus)
			return newEventbus
		}
	}
	return bus
}

func (c *client) Disconnect(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventbusCache.Range(func(key, value interface{}) bool {
		value.(api.Eventbus).Close(ctx)
		c.eventbusCache.Delete(key)
		return true
	})
}

func Connect(endpoints []string) Client {
	if len(endpoints) == 0 {
		return nil
	}
	return &client{
		Endpoints: endpoints,
		tracer:    tracing.NewTracer("client.client", trace.SpanKindClient),
	}
}

func GetEventbusIDIfNotSet(ctx context.Context, endpoints []string, opts *api.EventbusOptions) error {
	// the eventbus id does not exist, get the eventbus id first
	if opts.ID == uint64(0) {
		if opts.Name == "" {
			return errors.New("either eventbus name or id must be set")
		}
		// get eventbus id from name
		s := cluster.NewClusterController(endpoints, insecure.NewCredentials()).EventbusService()
		metaEventbus, err := s.GetSystemEventbusByName(ctx, opts.Name)
		if err != nil {
			return err
		}
		opts.ID = metaEventbus.Id
	}
	return nil
}
