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

package bare

import (
	// standard libraries
	"context"
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"

	// third-party libraries
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/net/connection"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

const (
	defaultConnectTimeoutMs = 300
)

func New(endpoint string, creator rpc.ClientCreator) rpc.Client {
	return &client{
		endpoint: endpoint,
		closed:   atomic.Bool{},
		mu:       sync.RWMutex{},
		creator:  creator,
		tracer:   tracing.NewTracer("internal.net.rpc.Client", trace.SpanKindClient),
	}
}

// client is a generic client of gRPC.
type client struct {
	endpoint string

	closed  atomic.Bool
	mu      sync.RWMutex
	conn    *grpc.ClientConn
	client  interface{}
	creator rpc.ClientCreator
	tracer  *tracing.Tracer
}

// make sure client implements rpc.Client.
var _ rpc.Client = (*client)(nil)

func (c *client) Endpoint() string {
	return c.endpoint
}

func (c *client) Get(ctx context.Context) (interface{}, error) {
	if c.closed.Load() {
		return nil, errors.ErrClosed
	}
	_ctx, span := c.tracer.Start(ctx, "Get")
	defer span.End()

	if client := c.cachedClient(); client != nil {
		return client, nil
	}
	return c.refreshClient(_ctx, false)
}

func (c *client) cachedClient() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.Ready() {
		return nil
	}

	return c.client
}

func (c *client) refreshClient(ctx context.Context, force bool) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, errors.ErrClosed
	}

	if !force && c.Ready() {
		return c.client, nil
	}

	// TODO: close previous connection
	c.doClose()

	// TODO: connect with opts
	connCtx, cancel := context.WithTimeout(ctx, defaultConnectTimeoutMs*time.Millisecond)
	defer cancel()
	conn, err := connection.Connect(connCtx, c.endpoint)
	if err != nil {
		return nil, err
	}
	client, err := c.creator.Create(conn)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	c.client = client
	return client, nil
}

func (c *client) Ready() bool {
	return c.conn != nil && c.conn.GetState() == connectivity.Ready
}

func (c *client) Close() {
	if c.closed.CAS(false, true) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.doClose()
	}
}

func (c *client) doClose() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
