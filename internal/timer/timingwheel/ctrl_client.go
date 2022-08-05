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

package timingwheel

import (
	"context"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/timer/errors"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ctrlClient struct {
	ctrlAddrs       []string
	grpcConn        map[string]*grpc.ClientConn
	ctrlClients     map[string]ctrlpb.EventBusControllerClient
	mutex           sync.Mutex
	leader          string
	leaderClient    ctrlpb.EventBusControllerClient
	credentials     credentials.TransportCredentials
	heartBeatClient ctrlpb.PingServerClient
}

func NewClient(ctrlAddrs []string) *ctrlClient {
	return &ctrlClient{
		ctrlAddrs:   ctrlAddrs,
		grpcConn:    map[string]*grpc.ClientConn{},
		ctrlClients: map[string]ctrlpb.EventBusControllerClient{},
		credentials: insecure.NewCredentials(),
	}
}

func (cli *ctrlClient) Close(ctx context.Context) {
	if len(cli.grpcConn) == 0 {
		return
	}
	for ip, conn := range cli.grpcConn {
		if err := conn.Close(); err != nil {
			log.Warning(ctx, "close grpc connection failed", map[string]interface{}{
				log.KeyError:   err,
				"peer_address": ip,
			})
		}
	}
}

func (cli *ctrlClient) heartbeat(ctx context.Context) error {
	var err error
	log.Debug(ctx, "heartbeat", map[string]interface{}{
		"leader": cli.leader,
	})
	if cli.heartBeatClient == nil {
		if cli.makeSureClient(ctx, true) == nil {
			return errors.ErrNoControllerLeader
		}
	}
	res, err := cli.heartBeatClient.Ping(context.Background(), &emptypb.Empty{})
	if err == nil && res.LeaderAddr == cli.leader {
		return nil
	}
	if cli.makeSureClient(ctx, true) == nil {
		return errors.ErrNoControllerLeader
	}
	return nil
}

func (cli *ctrlClient) makeSureClient(ctx context.Context, renew bool) ctrlpb.EventBusControllerClient {
	cli.mutex.Lock()
	defer cli.mutex.Unlock()
	if cli.leaderClient == nil || renew {
		leader := ""
		for _, v := range cli.ctrlAddrs {
			conn := cli.getGRPCConn(v)
			if conn == nil {
				continue
			}
			pingClient := ctrlpb.NewPingServerClient(conn)
			res, err := pingClient.Ping(ctx, &emptypb.Empty{})
			if err != nil {
				log.Info(ctx, "ping has error", map[string]interface{}{
					log.KeyError: err,
					"addr":       v,
				})
				continue
			}
			leader = res.LeaderAddr
			break
		}
		// todo check leader is invalid
		if leader == "" {
			log.Info(ctx, "leader is empty", nil)
			return nil
		}
		conn := cli.getGRPCConn(leader)
		if conn == nil {
			return nil
		}
		cli.leader = leader
		cli.leaderClient = ctrlpb.NewEventBusControllerClient(conn)
		cli.heartBeatClient = ctrlpb.NewPingServerClient(conn)
	}
	return cli.leaderClient
}

func (cli *ctrlClient) getGRPCConn(addr string) *grpc.ClientConn {
	ctx := context.Background()
	var err error
	conn := cli.grpcConn[addr]
	if isConnectionOK(conn) {
		return conn
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(cli.credentials))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithCancel(ctx)
	timeout := false
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		select {
		case <-ctx.Done():
		case <-ticker.C:
			cancel()
			timeout = true
		}
	}()
	conn, err = grpc.DialContext(ctx, addr, opts...)
	cancel()
	if timeout {
		log.Warning(ctx, "dial to controller timeout", map[string]interface{}{
			"ip": addr,
		})
	} else if err != nil {
		log.Warning(ctx, "dial to controller failed", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		cli.grpcConn[addr] = conn
		return conn
	}
	return nil
}

func isConnectionOK(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}
	return conn.GetState() == connectivity.Idle || conn.GetState() == connectivity.Ready
}
