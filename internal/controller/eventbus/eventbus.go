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
	"encoding/json"
	"fmt"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/observability"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
	"sync"
)

func NewEventBusController(cfg ControllerConfig) *controller {
	c := &controller{
		segmentPool: &segmentPool{},
		cfg:         &cfg,
	}
	return c
}

type controller struct {
	cfg                      *ControllerConfig
	kvStore                  kv.Client
	segmentPool              *segmentPool
	volumePool               *volumePool
	eventBusMap              map[string]*info.BusInfo
	eventLogInfo             map[string]*info.EventLogInfo
	segmentServerCredentials credentials.TransportCredentials
	segmentServerInfoMap     map[string]*info.SegmentServerInfo
	segmentServerConn        map[string]*grpc.ClientConn
	volumeInfoMap            map[string]*info.VolumeInfo
	segmentServerClientMap   map[string]segment.SegmentServerClient
}

func (ctrl *controller) Start() error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvStore = store
	if err = ctrl.segmentPool.init(ctrl); err != nil {
		return err
	}
	return ctrl.dynamicScaleUpEventLog()
}

func (ctrl *controller) Stop() error {
	return nil
}

func (ctrl *controller) CreateEventBus(ctx context.Context, req *ctrlpb.CreateEventBusRequest) (*meta.EventBus, error) {
	eb := &info.BusInfo{
		Namespace: req.Namespace,
		Name:      req.Name,
		LogNumber: 1, //req.LogNumber, force set to 1 temporary
		EventLogs: make([]*info.EventLogInfo, req.LogNumber),
	}
	eb.VRN = ctrl.generateEventBusVRN(eb)
	exist, err := ctrl.kvStore.Exists(eb.VRN.Value)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "invoke kv exist failed", err)
	}
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventbus resource name conflicted")
	}
	wg := sync.WaitGroup{}
	for idx := 0; idx < int(eb.LogNumber); idx++ {
		eb.EventLogs[idx] = &info.EventLogInfo{
			ID:                    int64(idx),
			EventBusVRN:           eb.VRN,
			CurrentSegmentNumbers: 0,
		}
		eb.EventLogs[idx].VRN = ctrl.generateEventLogVRN(eb.EventLogs[idx])
		wg.Add(1)
		// TODO thread safety
		// TODO asynchronous
		go func(i int) {
			_err := ctrl.initializeEventLog(ctx, eb.EventLogs[i])
			err = errors.Chain(err, _err)
			wg.Done()
		}(idx)
	}
	wg.Wait()

	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "initialized eventlog failed", err)
	}

	data, _ := json.Marshal(eb)
	ctrl.kvStore.Set(eb.VRN.Value, data)
	return &meta.EventBus{}, nil
}

func (ctrl *controller) DeleteEventBus(ctx context.Context,
	vrn *meta.VanusResourceName) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetEventBus(ctx context.Context,
	vrn *meta.VanusResourceName) (*meta.EventBus, error) {
	return &meta.EventBus{}, nil
}

func (ctrl *controller) UpdateEventBus(ctx context.Context,
	req *ctrlpb.UpdateEventBusRequest) (*meta.EventBus, error) {
	return &meta.EventBus{}, nil
}

func (ctrl *controller) ListSegment(ctx context.Context,
	vrn *meta.VanusResourceName) (*ctrlpb.ListSegmentResponse, error) {
	return &ctrlpb.ListSegmentResponse{}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	serverInfo, exist := ctrl.segmentServerInfoMap[req.Address]
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"segment server ip address is conflicted", nil)
	}
	ctrl.segmentServerInfoMap[req.Address] = serverInfo
	serverInfo.Address = req.Address
	volumeInfo, exist := ctrl.volumeInfoMap[req.VolumeId]
	if !exist {
		volumeInfo = ctrl.volumePool.get(req.VolumeId)
		if volumeInfo == nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
				"invalid volumeID, PVC not found", nil)
		}
	}
	if err := ctrl.volumePool.bindSegmentServer(volumeInfo, serverInfo); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"bind volume to segment server failed", err)
	}
	serverInfo.Volume = volumeInfo
	// TODO update state in KV store
	return &ctrlpb.RegisterSegmentServerResponse{
		ServerId:      serverInfo.ID(),
		SegmentBlocks: volumeInfo.Blocks,
	}, nil
}

func (ctrl *controller) UnregisterSegmentServer(ctx context.Context,
	req *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	serverInfo, exist := ctrl.segmentServerInfoMap[req.Address]
	if !exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"segment server not found", nil)
	}

	delete(ctrl.segmentServerInfoMap, serverInfo.Address)
	if err := ctrl.volumePool.release(serverInfo.Volume); err != nil {
		// TODO error handle
	}
	// TODO update state in KV store
	return &ctrlpb.UnregisterSegmentServerResponse{}, nil
}

func (ctrl *controller) QuerySegmentRouteInfo(ctx context.Context,
	req *ctrlpb.QuerySegmentRouteInfoRequest) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
	return &ctrlpb.QuerySegmentRouteInfoResponse{}, nil
}

func (ctrl *controller) SegmentHeartbeat(srv ctrlpb.SegmentController_SegmentHeartbeatServer) error {
	//srv.SendAndClose()
	return nil
}

func (ctrl *controller) initializeEventLog(ctx context.Context, el *info.EventLogInfo) error {
	ctrl.segmentPool.bindSegment(ctx, el, 3) // TODO eliminate magic number
	return nil
}

func (ctrl *controller) dynamicScaleUpEventLog() error {
	return nil
}

func (ctrl *controller) generateEventBusVRN(eb *info.BusInfo) *meta.VanusResourceName {
	return &meta.VanusResourceName{
		Value: strings.Join([]string{"eventbus", eb.Namespace, eb.Name}, ":"),
	}
}

func (ctrl *controller) generateEventLogVRN(el *info.EventLogInfo) *meta.VanusResourceName {
	return &meta.VanusResourceName{
		Value: strings.Join([]string{el.EventBusVRN.Value, "eventlog", fmt.Sprintf("%d", el.ID)}, ":"),
	}
}

func (ctrl *controller) getSegmentServerClient(i *info.SegmentServerInfo) segment.SegmentServerClient {
	cli := ctrl.segmentServerClientMap[i.ID()]
	if cli == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(ctrl.segmentServerCredentials))
		conn, err := grpc.Dial(i.Address, opts...)
		if err != nil {
			// TODO error handle
			return nil
		}
		ctrl.segmentServerConn[i.Address] = conn
		cli = segment.NewSegmentServerClient(conn)
		ctrl.segmentServerClientMap[i.ID()] = cli
	}
	return cli
}
