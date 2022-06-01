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

package eventbus

import (
	// standard libraries
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/client/pkg/errors"
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries
	ctlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
)

func newNameServiceImpl(endpoints []string) (*nameServiceImpl, error) {
	ns := &nameServiceImpl{
		endpoints:   endpoints,
		grpcConn:    map[string]*grpc.ClientConn{},
		ctrlClients: map[string]ctlpb.EventBusControllerClient{},
		mutex:       sync.Mutex{},
		credentials: insecure.NewCredentials(),
	}

	return ns, nil
}

type nameServiceImpl struct {
	//client       rpc.Client
	endpoints    []string
	grpcConn     map[string]*grpc.ClientConn
	ctrlClients  map[string]ctlpb.EventBusControllerClient
	leader       string
	leaderClient ctlpb.EventBusControllerClient
	mutex        sync.Mutex
	credentials  credentials.TransportCredentials
}

// make sure nameServiceImpl implements discovery.NameService.
var _ discovery.NameService = (*nameServiceImpl)(nil)

func (ns *nameServiceImpl) LookupWritableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	// TODO: use list
	// req := &ctlpb.ListEventLogsRequest{
	// 	Parent:       eventbus,
	// 	WritableOnly: true,
	// }
	// resp, err := ns.client.ListEventLogs(context.Background(), req)
	req := &metapb.EventBus{
		Id:   eventbus.ID,
		Name: eventbus.Name,
	}

	var resp *metapb.EventBus
	var err = errors.ErrNoControllerLeader
	client := ns.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	resp, err = client.GetEventBus(ctx, req)
	if ns.isNeedRetry(err) {
		client = ns.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		resp, err = client.GetEventBus(ctx, req)
	}

	if err != nil {
		return nil, err
	}
	return toLogs(resp.GetLogs()), nil
}

func (ns *nameServiceImpl) LookupReadableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	// TODO: use list
	// req := &ctlpb.ListEventLogsRequest{
	// 	Parent:       eventbus,
	// 	ReadableOnly: true,
	// }
	// resp, err := ns.client.ListEventLogs(context.Background(), req)
	req := &metapb.EventBus{
		Id:   eventbus.ID,
		Name: eventbus.Name,
	}

	var resp *metapb.EventBus
	var err = errors.ErrNoControllerLeader
	client := ns.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	resp, err = client.GetEventBus(ctx, req)
	if ns.isNeedRetry(err) {
		client = ns.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		resp, err = client.GetEventBus(ctx, req)
	}

	if err != nil {
		return nil, err
	}

	return toLogs(resp.GetLogs()), nil
}

func (ns *nameServiceImpl) makeSureClient(renew bool) ctlpb.EventBusControllerClient {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()
	if ns.leaderClient == nil || renew {
		leader := ""
		for _, v := range ns.endpoints {
			conn := ns.getGRPCConn(v)
			if conn == nil {
				continue
			}
			pingClient := ctlpb.NewPingServerClient(conn)
			res, err := pingClient.Ping(context.Background(), &emptypb.Empty{})
			if err != nil {
				fmt.Printf("ping failed: %s\n", err)
				return nil
			}
			leader = res.LeaderAddr
			fmt.Printf("get leader address: %s\n", res.LeaderAddr)
			break
		}

		conn := ns.getGRPCConn(leader)
		if conn == nil {
			return nil
		}
		ns.leader = leader
		ns.leaderClient = ctlpb.NewEventBusControllerClient(conn)
	}
	return ns.leaderClient
}

func (ns *nameServiceImpl) getGRPCConn(addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	ctx := context.Background()
	var err error
	conn := ns.grpcConn[addr]
	if isConnectionOK(conn) {
		return conn
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(ns.credentials))
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
		// TODO use log thirds
		fmt.Printf("dial to: %s controller timeout\n", addr)
	} else if err != nil {
		fmt.Printf("dial to: %s controller failed, error:%s\n", addr, err)
	} else {
		ns.grpcConn[addr] = conn
		return conn
	}
	return nil
}

func (ns *nameServiceImpl) isNeedRetry(err error) bool {
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

func toLogs(logpbs []*metapb.EventLog) []*record.EventLog {
	if len(logpbs) <= 0 {
		return make([]*record.EventLog, 0, 0)
	}
	logs := make([]*record.EventLog, 0, len(logpbs))
	for _, logpb := range logpbs {
		logs = append(logs, toLog(logpb))
	}
	return logs
}

func toLog(logpb *metapb.EventLog) *record.EventLog {
	addrs := logpb.GetServerAddress()
	if len(addrs) <= 0 {
		// FIXME: missing address
		addrs = []string{"localhost:2048"}
	}
	sort.Strings(addrs)
	controllers := strings.Join(addrs, ",")
	log := &record.EventLog{
		// TODO: format of vrn
		VRN:  fmt.Sprintf("vanus:///eventlog/%d?eventbus=%s&controllers=%s", logpb.GetEventLogId(), logpb.GetEventBusName(), controllers),
		Mode: record.PremWrite | record.PremRead,
		// Mode: record.LogMode(logpb.GetMode()),
	}
	return log
}
