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

package server

import (
	"context"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"
)

type Manager interface {
	AddServer(ctx context.Context, srv Server) error
	RemoveServer(ctx context.Context, srv Server) error
	GetServerByAddress(addr string) Server
	GetServerByServerID(id vanus.ID) Server
	Run(ctx context.Context) error
	Stop(ctx context.Context)
}

func newSegmentServerManager() Manager {
	return &segmentServerManager{
		segmentServerCredentials: insecure.NewCredentials(),
	}
}

type segmentServerManager struct {
	segmentServerCredentials credentials.TransportCredentials
	// map[string]Server
	segmentServerMapByIP sync.Map
	// map[vanus.ID]Server
	segmentServerMapByID sync.Map
	cancelCtx            context.Context
	cancel               func()
}

func (mgr *segmentServerManager) AddServer(ctx context.Context, srv Server) error {
	if srv == nil {
		return nil
	}

	mgr.segmentServerMapByIP.Store(srv.Address(), srv)
	mgr.segmentServerMapByID.Store(srv.ID(), srv)
	return nil
}

func (mgr *segmentServerManager) RemoveServer(ctx context.Context, srv Server) error {
	if srv == nil {
		return nil
	}

	mgr.segmentServerMapByIP.Delete(srv.Address())
	mgr.segmentServerMapByID.Delete(srv.ID())
	return nil
}

func (mgr *segmentServerManager) GetServerByAddress(addr string) Server {
	v, exist := mgr.segmentServerMapByIP.Load(addr)
	if exist {
		return v.(Server)
	}
	return nil
}

func (mgr *segmentServerManager) GetServerByServerID(id vanus.ID) Server {
	v, exist := mgr.segmentServerMapByID.Load(id)
	if exist {
		return v.(Server)
	}
	return nil
}

func (mgr *segmentServerManager) Run(ctx context.Context) error {
	newCtx := context.Background()
	for {
		select {
		case <-mgr.cancelCtx.Done():
			return nil
		default:
		}
		mgr.segmentServerMapByIP.Range(func(key, value interface{}) bool {
			srv, ok := value.(Server)
			if !ok {
				mgr.segmentServerMapByIP.Delete(key)
			}
			if !srv.IsActive() {
				mgr.segmentServerMapByIP.Delete(srv.Address())
				mgr.segmentServerMapByID.Delete(srv.ID())
				log.Info(newCtx, "the server isn't active", map[string]interface{}{
					"id":      srv.ID(),
					"address": srv.Address(),
					"up_time": srv.Uptime(),
				})
			}
			return true
		})
		time.Sleep(time.Second)
	}
}

func (mgr *segmentServerManager) Stop(ctx context.Context) {
	mgr.cancel()
	mgr.segmentServerMapByIP.Range(func(key, value interface{}) bool {
		srv, ok := value.(Server)
		if !ok {
			mgr.segmentServerMapByIP.Delete(key)
		}

		err := srv.(*segmentServer).grpcConn.Close()
		if err != nil {
			log.Warning(ctx, "close grpc connection error", map[string]interface{}{
				"id":         srv.ID(),
				"address":    srv.Address(),
				"up_time":    srv.Uptime(),
				log.KeyError: err,
			})
		} else {
			log.Info(ctx, "the connection to server was closed", map[string]interface{}{
				"id":      srv.ID(),
				"address": srv.Address(),
				"up_time": srv.Uptime(),
			})
		}

		return true
	})
}

type Server interface {
	RemoteStart(ctx context.Context) error
	RemoteStop(ctx context.Context) error
	GetClient() segpb.SegmentServerClient
	ID() vanus.ID
	Address() string
	Close() error
	Polish()
	IsActive() bool
	Uptime() time.Time
}

type segmentServer struct {
	id                vanus.ID
	addr              string
	grpcConn          *grpc.ClientConn
	client            segpb.SegmentServerClient
	uptime            time.Time
	lastHeartbeatTime time.Time
}

func NewSegmentServer(addr string) (Server, error) {
	srv := &segmentServer{
		addr: addr,
		id:   vanus.NewID(),
	}
	var opts []grpc.DialOption
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	srv.grpcConn = conn
	srv.client = segpb.NewSegmentServerClient(conn)
	return srv, nil
}

func (ss *segmentServer) RemoteStart(ctx context.Context) error {
	return nil
}

func (ss *segmentServer) RemoteStop(ctx context.Context) error {
	return nil
}

func (ss *segmentServer) GetClient() segpb.SegmentServerClient {
	return ss.client
}

func (ss *segmentServer) ID() vanus.ID {
	return ss.id
}

func (ss *segmentServer) Address() string {
	return ss.addr
}

func (ss *segmentServer) Close() error {
	return ss.grpcConn.Close()
}

func (ss *segmentServer) Polish() {
	ss.lastHeartbeatTime = time.Now()
}

func (ss *segmentServer) IsActive() bool {
	// maximum heartbeat interval is 1 minute
	return time.Now().Sub(ss.lastHeartbeatTime) > time.Minute
}

func (ss *segmentServer) Uptime() time.Time {
	return ss.uptime
}
