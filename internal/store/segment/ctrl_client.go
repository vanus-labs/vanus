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

package segment

import (
	// standard libraries.
	"context"
	"io"
	"sync"
	"time"

	// third-party libraries.
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	"github.com/linkall-labs/vanus/internal/store/errors"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
)

type ctrlClient struct {
	ctrlAddrs       []string
	grpcConn        map[string]*grpc.ClientConn
	ctrlClients     map[string]ctrlpb.SegmentControllerClient
	mutex           sync.Mutex
	leader          string
	leaderClient    ctrlpb.SegmentControllerClient
	credentials     credentials.TransportCredentials
	heartBeatClient ctrlpb.SegmentController_SegmentHeartbeatClient
	// pingClient      ctrlpb.PingServerClient
}

func NewClient(ctrlAddrs []string) *ctrlClient {
	return &ctrlClient{
		ctrlAddrs:   ctrlAddrs,
		grpcConn:    map[string]*grpc.ClientConn{},
		ctrlClients: map[string]ctrlpb.SegmentControllerClient{},
		credentials: insecure.NewCredentials(),
	}
}

func (cli *ctrlClient) Close(ctx context.Context) {
	if len(cli.grpcConn) == 0 {
		return
	}
	if cli.heartBeatClient != nil {
		if _, err := cli.heartBeatClient.CloseAndRecv(); err != nil {
			log.Warning(ctx, "close gRPC stream error", map[string]interface{}{
				log.KeyError: err,
			})
		}
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

func (cli *ctrlClient) registerSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest,
) (*ctrlpb.RegisterSegmentServerResponse, error) {
	client := cli.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	res, err := client.RegisterSegmentServer(ctx, req)
	if cli.isNeedRetry(err) {
		client = cli.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.RegisterSegmentServer(ctx, req)
	}
	return res, err
}

//func (cli *ctrlClient) unregisterSegmentServer(ctx context.Context,
//	req *ctrlpb.UnregisterSegmentServerRequest,
//) (*ctrlpb.UnregisterSegmentServerResponse, error) {
//	client := cli.makeSureClient(false)
//	if client == nil {
//		return nil, errors.ErrNoControllerLeader
//	}
//	res, err := client.UnregisterSegmentServer(ctx, req)
//	if cli.isNeedRetry(err) {
//		client = cli.makeSureClient(true)
//		if client == nil {
//			return nil, errors.ErrNoControllerLeader
//		}
//		res, err = client.UnregisterSegmentServer(ctx, req)
//	}
//	return res, err
//}

func (cli *ctrlClient) reportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest,
) (*emptypb.Empty, error) {
	client := cli.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	res, err := client.ReportSegmentBlockIsFull(ctx, req)
	if cli.isNeedRetry(err) {
		client = cli.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.ReportSegmentBlockIsFull(ctx, req)
	}
	return res, err
}

func (cli *ctrlClient) reportSegmentLeader(ctx context.Context, req *ctrlpb.ReportSegmentLeaderRequest) error {
	client := cli.makeSureClient(false)
	if client == nil {
		return errors.ErrNoControllerLeader
	}
	_, err := client.ReportSegmentLeader(ctx, req)
	if cli.isNeedRetry(err) {
		client = cli.makeSureClient(true)
		if client == nil {
			return errors.ErrNoControllerLeader
		}
		_, err = client.ReportSegmentLeader(ctx, req)
	}
	return err
}

func (cli *ctrlClient) heartbeat(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	log.Debug(ctx, "heartbeat", map[string]interface{}{
		"leader": cli.leader,
	})
	var err error
	if cli.heartBeatClient == nil {
		client := cli.makeSureClient(false)
		if client == nil {
			return errors.ErrNoControllerLeader
		}
		cli.heartBeatClient, err = client.SegmentHeartbeat(ctx)
		if err != nil {
			sts := status.Convert(err)
			if sts.Code() == codes.Unavailable {
				client = cli.makeSureClient(true)
				if client == nil {
					return errors.ErrNoControllerLeader
				}
				cli.heartBeatClient, err = client.SegmentHeartbeat(ctx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	err = cli.heartBeatClient.Send(req)
	if err == io.EOF || cli.isNeedRetry(err) {
		client := cli.makeSureClient(true)
		if client == nil {
			return errors.ErrNoControllerLeader
		}
		cli.heartBeatClient, err = client.SegmentHeartbeat(ctx)
		if err != nil {
			return err
		}
		err = cli.heartBeatClient.Send(req)
	}

	return err
}

func (cli *ctrlClient) makeSureClient(renew bool) ctrlpb.SegmentControllerClient {
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
			res, err := pingClient.Ping(context.Background(), &emptypb.Empty{})
			if err != nil {
				log.Info(context.Background(), "ping has error", map[string]interface{}{
					log.KeyError: err,
					"addr":       v,
				})
				continue
			}
			leader = res.LeaderAddr
			break
		}

		conn := cli.getGRPCConn(leader)
		if conn == nil {
			return nil
		}
		cli.leader = leader
		cli.leaderClient = ctrlpb.NewSegmentControllerClient(conn)
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
		ticker := time.Tick(time.Second)
		select {
		case <-ctx.Done():
		case <-ticker:
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

func (cli *ctrlClient) isNeedRetry(err error) bool {
	if err == nil {
		return false
	}
	if err == errors.ErrNoControllerLeader {
		return true
	}
	sts := status.Convert(err)
	if sts == nil {
		return false
	}
	errType, ok := errpb.Convert(sts.Message())
	if !ok {
		return false
	}
	if errType.Code == errpb.ErrorCode_NOT_LEADER {
		return true
	}
	return false
}

func isConnectionOK(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}
	return conn.GetState() == connectivity.Idle || conn.GetState() == connectivity.Ready
}
