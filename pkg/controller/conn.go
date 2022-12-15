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

package controller

import (
	"context"
	stderr "errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/errors"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	vanusConnBypass = "VANUS_CONN_BYPASS"
)

type conn struct {
	mutex        sync.Mutex
	leader       string
	leaderClient *grpc.ClientConn
	endpoints    []string
	credentials  credentials.TransportCredentials
	grpcConn     map[string]*grpc.ClientConn
	bypass       bool
}

func newConn(endpoints []string, credentials credentials.TransportCredentials) *conn {
	// TODO temporary implement
	v, _ := strconv.ParseBool(os.Getenv(vanusConnBypass))
	log.Info(context.Background(), "init conn", map[string]interface{}{
		"endpoints": endpoints,
	})
	return &conn{
		endpoints:   endpoints,
		grpcConn:    map[string]*grpc.ClientConn{},
		credentials: credentials,
		bypass:      v,
	}
}

func (c *conn) invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	log.Debug(ctx, "grpc invoke", map[string]interface{}{
		"method": method,
		"args":   fmt.Sprintf("%v", args),
	})
	conn := c.makeSureClient(ctx, false)
	if conn == nil {
		log.Warning(ctx, "not get client for controller", map[string]interface{}{})
		return errors.ErrNoControllerLeader
	}
	err := conn.Invoke(ctx, method, args, reply, opts...)
	if err != nil {
		log.Warning(ctx, "invoke error, try to retry", map[string]interface{}{
			log.KeyError: err,
		})
	}
	if isNeedRetry(err) {
		conn = c.makeSureClient(ctx, true)
		if conn == nil {
			log.Warning(ctx, "not get client when try to renew client", map[string]interface{}{})
			return errors.ErrNoControllerLeader
		}
		err = conn.Invoke(ctx, method, args, reply, opts...)
	}
	if err != nil {
		log.Warning(ctx, "invoke error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return err
}

func (c *conn) close() error {
	var err error
	for ip, conn := range c.grpcConn {
		if _err := conn.Close(); _err != nil {
			log.Info(context.Background(), "close grpc connection failed", map[string]interface{}{
				log.KeyError:   _err,
				"peer_address": ip,
			})
			err = errors.Chain(err, _err)
		}
	}
	return err
}

func (c *conn) makeSureClient(ctx context.Context, renew bool) *grpc.ClientConn {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.leaderClient == nil || renew {
		if c.bypass {
			c.leaderClient = c.getGRPCConn(ctx, c.endpoints[0])
			return c.leaderClient
		}
		log.Info(ctx, "try to create connection", map[string]interface{}{
			"renew":     renew,
			"endpoints": c.endpoints,
		})
		for _, v := range c.endpoints {
			conn := c.getGRPCConn(ctx, v)
			if conn == nil {
				continue
			}
			pingClient := ctrlpb.NewPingServerClient(conn)
			res, err := pingClient.Ping(context.Background(), &emptypb.Empty{})
			if err != nil {
				log.Info(ctx, "failed to ping controller", map[string]interface{}{
					"address":    v,
					log.KeyError: err,
				})
				return nil
			}
			c.leader = res.LeaderAddr
			if v == res.LeaderAddr {
				c.leaderClient = conn
				return conn
			}
			break
		}

		conn := c.getGRPCConn(ctx, c.leader)
		if conn == nil {
			log.Info(ctx, "failed to get conn", map[string]interface{}{})
			return nil
		}
		log.Info(ctx, "success to get connection", map[string]interface{}{
			"leader": c.leader,
		})
		c.leaderClient = conn
	}
	return c.leaderClient
}

func (c *conn) getGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	var err error
	conn := c.grpcConn[addr]
	if isConnectionOK(conn) {
		return conn
	} else if conn != nil {
		_ = conn.Close() // make sure it's closed
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(c.credentials))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	conn, err = grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		log.Error(ctx, "failed to dial to controller", map[string]interface{}{
			"address":    addr,
			log.KeyError: err,
		})
		return nil
	}
	c.grpcConn[addr] = conn
	return conn
}

func isNeedRetry(err error) bool {
	if err == nil {
		return false
	}
	if stderr.Is(err, errors.ErrNoControllerLeader) {
		return true
	}
	sts := status.Convert(err)
	if sts == nil {
		return false
	}
	if sts.Code() == codes.Unavailable {
		return true
	}

	if strings.Contains(sts.Message(), "NOT_LEADER") {
		return true
	}
	//errType, ok := errpb.Convert(sts.Message())
	//if !ok {
	//	return false
	//}
	//if errType.Code == errpb.ErrorCode_NOT_LEADER {
	//	log.Info(nil, "ErrorCode_NOT_LEADER", map[string]interface{}{
	//		log.KeyError: err,
	//	})
	//	return true
	//}
	return false
}

func isConnectionOK(conn *grpc.ClientConn) bool {
	if conn == nil {
		return false
	}
	return conn.GetState() == connectivity.Idle || conn.GetState() == connectivity.Ready
}
