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

package eventlog

import (
	// standard libraries.
	"context"
	stderr "errors"
	"fmt"
	"math"
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
	ctlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	// this project.
	vdr "github.com/linkall-labs/vanus/client/internal/vanus/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

func newNameServiceImpl(endpoints []string) (*nameServiceImpl, error) {
	ns := &nameServiceImpl{
		endpoints:   endpoints,
		grpcConn:    map[string]*grpc.ClientConn{},
		ctrlClients: map[string]ctlpb.EventLogControllerClient{},
		mutex:       sync.Mutex{},
		credentials: insecure.NewCredentials(),
	}
	// TODO: non-blocking now
	// if _, err := ns.Client(); err != nil {
	// 	return nil, err
	// }
	return ns, nil
}

type nameServiceImpl struct {
	// client       rpc.Client
	endpoints    []string
	grpcConn     map[string]*grpc.ClientConn
	ctrlClients  map[string]ctlpb.EventLogControllerClient
	leader       string
	leaderClient ctlpb.EventLogControllerClient
	mutex        sync.Mutex
	credentials  credentials.TransportCredentials
}

func (ns *nameServiceImpl) LookupWritableSegment(ctx context.Context, eventlog *discovery.VRN) (*vdr.LogSegment, error) {
	// TODO: use standby segments
	req := &ctlpb.GetAppendableSegmentRequest{
		EventLogId: eventlog.ID,
		Limited:    1,
	}

	var resp *ctlpb.GetAppendableSegmentResponse
	client := ns.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	resp, err := client.GetAppendableSegment(ctx, req)
	if ns.isNeedRetry(err) {
		client = ns.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		resp, err = client.GetAppendableSegment(ctx, req)
	}
	if err != nil {
		return nil, err
	}

	segments := toSegments(resp.GetSegments())
	if len(segments) == 0 {
		return nil, errors.ErrNotWritable
	}
	return segments[0], nil
}

func (ns *nameServiceImpl) LookupReadableSegments(ctx context.Context, eventlog *discovery.VRN) ([]*vdr.LogSegment, error) {
	// TODO: use range
	req := &ctlpb.ListSegmentRequest{
		EventLogId:  eventlog.ID,
		StartOffset: 0,
		EndOffset:   math.MaxInt64,
		Limited:     math.MaxInt32,
	}

	var resp *ctlpb.ListSegmentResponse
	client := ns.makeSureClient(false)
	if client == nil {
		return nil, errors.ErrNoControllerLeader
	}
	resp, err := client.ListSegment(ctx, req)
	if ns.isNeedRetry(err) {
		client = ns.makeSureClient(true)
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		resp, err = client.ListSegment(ctx, req)
	}
	if err != nil {
		return nil, err
	}

	segments := toSegments(resp.GetSegments())
	return segments, nil
}

func (ns *nameServiceImpl) makeSureClient(renew bool) ctlpb.EventLogControllerClient {
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
				return nil
			}
			leader = res.LeaderAddr
			break
		}

		conn := ns.getGRPCConn(leader)
		if conn == nil {
			return nil
		}
		ns.leader = leader
		ns.leaderClient = ctlpb.NewEventLogControllerClient(conn)
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
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err = grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		// TODO use log thirds
		if stderr.Is(err, context.DeadlineExceeded) {
			fmt.Printf("dial to: %s controller timeout\n", addr)
		} else {
			fmt.Printf("dial to: %s controller failed, error:%s\n", addr, err)
		}
		return nil
	}
	ns.grpcConn[addr] = conn
	return conn
}

func (ns *nameServiceImpl) isNeedRetry(err error) bool {
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

func toSegments(segmentpbs []*metapb.Segment) []*vdr.LogSegment {
	if len(segmentpbs) == 0 {
		return make([]*vdr.LogSegment, 0)
	}
	segments := make([]*vdr.LogSegment, 0, len(segmentpbs))
	for _, segmentpb := range segmentpbs {
		segment := toSegment(segmentpb)
		segments = append(segments, segment)
		// only return first working segment
		if segment.Writable {
			break
		}
	}
	return segments
}

func toSegment(segmentpb *metapb.Segment) *vdr.LogSegment {
	blocks := make(map[uint64]*vdr.SegmentBlock, len(segmentpb.Replicas))
	for blockID, blockpb := range segmentpb.Replicas {
		blocks[blockID] = &vdr.SegmentBlock{
			ID:       blockpb.Id,
			Endpoint: blockpb.Endpoint,
		}
	}
	segment := &vdr.LogSegment{
		ID:          segmentpb.GetId(),
		StartOffset: segmentpb.GetStartOffsetInLog(),
		// TODO align to server side
		EndOffset: segmentpb.GetEndOffsetInLog() + 1,
		// TODO: writable
		Writable:      segmentpb.State == "working",
		Blocks:        blocks,
		LeaderBlockID: segmentpb.GetLeaderBlockId(),
	}
	return segment
}
