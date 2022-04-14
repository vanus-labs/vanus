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

package volume

import (
	"context"
	"github.com/linkall-labs/vanus/internal/kv"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ServerManager interface {
	AddServer(ctx context.Context, address string) (Server, error)
	RemoveServer(ctx context.Context, srv Server) error
	GetServerInfoByAddress(addr string) Server
	GetServerInfoByServerID(addr string) Server
	Init(ctx context.Context, kvClient kv.Client) error
	Polish(server Server)
}

func newSegmentServerManager() *segmentServerManager {
	return &segmentServerManager{
		segmentServerCredentials: insecure.NewCredentials(),
		segmentServerInfoMap:     map[string]Server{},
	}
}

type segmentServerManager struct {
	segmentServerCredentials credentials.TransportCredentials
	segmentServerInfoMap     map[string]Server
}

func (mgr *segmentServerManager) start(ctx context.Context) error {
	return nil
}

func (mgr *segmentServerManager) stop(ctx context.Context) {

}

func (mgr *segmentServerManager) AddServer(ctx context.Context, address string) (Server, error) {
	return nil, nil
}

func (mgr *segmentServerManager) RemoveServer(ctx context.Context, srv Server) error {
	return nil
}

func (mgr *segmentServerManager) GetServerInfoByAddress(addr string) Server {
	return nil
}

func (mgr *segmentServerManager) GetServerInfoByServerID(id string) Server {
	return nil
}

func (mgr *segmentServerManager) polish(srv Server) {

}

func (mgr *segmentServerManager) remoteStartServer(srv Server) {

}

type Server interface {
	RemoteStart(ctx context.Context) error
	RemoteStop(ctx context.Context) error
	GetClient() segpb.SegmentServerClient
	ID() uint64
	Address() string
}
