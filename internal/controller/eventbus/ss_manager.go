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

package eventbus

import (
	"context"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/observability/log"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func newSegmentServerManager() *segmentServerManager {
	return &segmentServerManager{
		segmentServerCredentials: insecure.NewCredentials(),
		segmentServerInfoMap:     map[string]*info.SegmentServerInfo{},
		segmentServerConn:        map[string]*grpc.ClientConn{},
		segmentServerClientMap:   map[string]segpb.SegmentServerClient{},
	}
}

type segmentServerManager struct {
	segmentServerCredentials credentials.TransportCredentials
	segmentServerInfoMap     map[string]*info.SegmentServerInfo
	// key: address
	// value: connection to SegmentServer
	segmentServerConn      map[string]*grpc.ClientConn
	segmentServerClientMap map[string]segpb.SegmentServerClient
}

func (mgr *segmentServerManager) start(ctx context.Context) error {
	return nil
}

func (mgr *segmentServerManager) stop(ctx context.Context) {

}

func (mgr *segmentServerManager) getServerInfos() []*info.SegmentServerInfo {
	return nil
}

func (mgr *segmentServerManager) AddServer(ctx context.Context, address string) (*info.SegmentServerInfo, error) {
	return nil, nil
}

func (mgr *segmentServerManager) RemoveServer(ctx context.Context, info *info.SegmentServerInfo) error {
	return nil
}

func (mgr *segmentServerManager) GetServerInfoByAddress(addr string) *info.SegmentServerInfo {
	return nil
}

func (mgr *segmentServerManager) GetServerInfoByServerID(id string) *info.SegmentServerInfo {
	return nil
}

func (mgr *segmentServerManager) polish(info *info.SegmentServerInfo) {

}

func (mgr *segmentServerManager) remoteStartServer(info *info.SegmentServerInfo) {

}

func (mgr *segmentServerManager) getSegmentServerClient(i *info.SegmentServerInfo) segpb.SegmentServerClient {
	if i == nil {
		return nil
	}
	cli := mgr.segmentServerClientMap[i.ID()]
	if cli == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(mgr.segmentServerCredentials))
		conn, err := grpc.Dial(i.Address, opts...)
		if err != nil {
			// TODO error handle
			return nil
		}
		mgr.segmentServerConn[i.Address] = conn
		cli = segpb.NewSegmentServerClient(conn)
		mgr.segmentServerClientMap[i.ID()] = cli
	}
	return cli
}

func (mgr *segmentServerManager) readyToStartSegmentServer(ctx context.Context, serverInfo *info.SegmentServerInfo) {
	conn := mgr.getSegmentServerClient(serverInfo)
	if conn == nil {
		return
	}
	_, err := conn.Start(ctx, &segpb.StartSegmentServerRequest{
		SegmentServerId: uuid.NewString(),
	})
	if err != nil {
		log.Warning(ctx, "start segment server failed", map[string]interface{}{
			log.KeyError: err,
			"address":    serverInfo.Address,
		})
	}
}
