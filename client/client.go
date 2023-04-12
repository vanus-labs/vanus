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
	"google.golang.org/grpc/credentials/insecure"

	// this project.
	eb "github.com/vanus-labs/vanus/client/internal/eventbus"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventbus"
)

type client struct {
	endpoints []string
	cache     sync.Map

	mu     sync.RWMutex
	tracer *tracing.Tracer
}

func (c *client) Eventbus(ctx context.Context, opts ...api.EventbusOption) (api.Eventbus, error) {
	_, span := c.tracer.Start(ctx, "EventbusService")
	defer span.End()

	defaultOpts := api.DefaultEventbusOptions()
	for _, apply := range opts {
		apply(defaultOpts)
	}

	err := GetEventbusIDIfNotSet(ctx, c.endpoints, defaultOpts)
	if err != nil {
		log.Error(ctx).Err(err).
			Str("eventbus_name", defaultOpts.Name).
			Uint64("eventbus_id", defaultOpts.ID).
			Msg("get eventbus id failed")
		return nil, err
	}

	bus := func() api.Eventbus {
		if value, ok := c.cache.Load(defaultOpts.ID); ok {
			value.(*eventbus.Eventbus).Acquire()
			return value.(api.Eventbus)
		} else {
			return nil
		}
	}()

	if bus == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if value, ok := c.cache.Load(defaultOpts.ID); ok { // double check
			return value.(api.Eventbus), nil
		} else {
			cfg := &eb.Config{
				Endpoints: c.endpoints,
				ID:        defaultOpts.ID,
			}
			newEventbus := eventbus.NewEventbus(cfg, c.close)
			newEventbus.Acquire()
			c.cache.Store(defaultOpts.ID, newEventbus)
			return newEventbus, nil
		}
	}
	return bus, nil
}

func (c *client) Disconnect(ctx context.Context) {
	c.cache.Range(func(key, value interface{}) bool {
		value.(api.Eventbus).Close(ctx)
		return true
	})
}

func (c *client) close(id uint64) {
	c.cache.Delete(id)
}

func Connect(endpoints []string) api.Client {
	if len(endpoints) == 0 {
		return nil
	}
	return &client{
		endpoints: endpoints,
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
