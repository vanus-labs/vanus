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

package gateway

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mwitkow/grpc-proxy/proxy"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

func newCtrlProxy(port int, allowProxyMethod map[string]string, ctrlList []string) *ctrlProxy {
	return &ctrlProxy{
		ctrlLists:        ctrlList,
		port:             port,
		allowProxyMethod: allowProxyMethod,
		ticker:           time.NewTicker(time.Second),
		tracer:           tracing.NewTracer("controller-proxy", trace.SpanKindServer),
	}
}

type ctrlProxy struct {
	ctrlLists        []string
	rwMutex          sync.RWMutex
	leaderConn       *grpc.ClientConn
	allowProxyMethod map[string]string
	ticker           *time.Ticker
	port             int
	tracer           *tracing.Tracer
}

func (cp *ctrlProxy) start(ctx context.Context) error {
	grpcServer := grpc.NewServer(
		grpc.UnknownServiceHandler(proxy.TransparentHandler(cp.director)),
	)
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cp.port))
	if err != nil {
		return err
	}

	go cp.updateLeader(ctx)
	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			panic("start grpc server failed: " + err.Error())
		}
		log.Info(ctx, "the grpc server shutdown", nil)
	}()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()
	go cp.updateLeader(ctx)
	return nil
}

func (cp *ctrlProxy) director(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	outCtx := metadata.NewOutgoingContext(ctx, md.Copy())

	if cp.leaderConn == nil {
		return outCtx, nil, status.Errorf(codes.Internal, "No leader founded")
	}

	_, exist := cp.allowProxyMethod[fullMethodName]
	if !exist {
		log.Warning(ctx, "invalid access", map[string]interface{}{
			"method": fullMethodName,
		})
		return outCtx, nil, status.Errorf(codes.Unimplemented, "Unknown method")
	}

	cp.rwMutex.RLock()
	defer cp.rwMutex.RUnlock()
	return outCtx, cp.leaderConn, nil
}

func (cp *ctrlProxy) updateLeader(ctx context.Context) {
	defer cp.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.ticker.C:
			cp.rwMutex.Lock()
			if cp.leaderConn == nil {
				cp.leaderConn = getLeaderControllerGRPCConn(ctx, cp.ctrlLists...)
			} else {
				pingClient := ctrlpb.NewPingServerClient(cp.leaderConn)
				res, err := pingClient.Ping(ctx, &emptypb.Empty{})
				if err != nil {
					cp.leaderConn = getLeaderControllerGRPCConn(ctx, cp.ctrlLists...)
				} else if res.LeaderAddr != cp.leaderConn.Target() {
					cp.leaderConn = getLeaderControllerGRPCConn(ctx, res.LeaderAddr)
				}
			}
			if cp.leaderConn == nil {
				log.Error(ctx, "connect to leader failed", nil)
			}
			cp.rwMutex.Unlock()
		}
	}
}

func getLeaderControllerGRPCConn(ctx context.Context, endpoints ...string) *grpc.ClientConn {
	var leaderAddr string
	var leaderConn *grpc.ClientConn
	tryConnectLeaderOnce := false
	for idx := range endpoints {
		conn := createGRPCConn(ctx, endpoints[idx])
		if conn == nil {
			continue
		}
		pingClient := ctrlpb.NewPingServerClient(conn)
		res, err := pingClient.Ping(ctx, &emptypb.Empty{})
		if err != nil {
			log.Warning(ctx, "ping controller failed", map[string]interface{}{
				"endpoint": endpoints[idx],
			})
			continue
		}
		leaderAddr = res.LeaderAddr
		if leaderAddr == endpoints[idx] {
			leaderConn = conn
			tryConnectLeaderOnce = false
		} else {
			_ = conn.Close()
		}
		break
	}

	if leaderAddr == "" {
		return nil
	}

	if leaderConn != nil {
		return leaderConn
	} else if !tryConnectLeaderOnce {
		leaderConn = createGRPCConn(ctx, leaderAddr)
	}

	if leaderConn == nil {
		log.Error(ctx, "ping controller failed", nil)
		return nil
	}
	return leaderConn
}

func createGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	var err error
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithCancel(ctx)
	timeout := false
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		select {
		case <-ctx.Done():
		case <-ticker.C:
			cancel()
			timeout = true
		}
	}()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	cancel()
	if timeout {
		log.Warning(ctx, "dial to controller timeout, try to another controller", map[string]interface{}{
			"endpoint": addr,
		})
		return nil
	} else if err != nil {
		log.Warning(ctx, "dial to controller failed, try to another controller", map[string]interface{}{
			"endpoint": addr,
		})
		return nil
	}
	return conn
}
