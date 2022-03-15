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
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultAutoCreatedSegmentNumber = 3
	eventbusKeyPrefixInKVStore      = "/vanus/internal/resource/eventbus"
	eventlogKeyPrefixInKVStore      = "/vanus/internal/resource/eventlog"
	segmentBlockKeyPrefixInKVStore  = "/vanus/internal/resource/segmentBlock"
	volumeKeyPrefixInKVStore        = "/vanus/internal/resource/volume"
)

func NewEventBusController(cfg ControllerConfig) *controller {
	c := &controller{
		cfg:                      &cfg,
		volumePool:               &volumePool{},
		eventBusMap:              map[string]*info.BusInfo{},
		segmentServerCredentials: insecure.NewCredentials(),
		segmentServerInfoMap:     map[string]*info.SegmentServerInfo{},
		segmentServerConn:        map[string]*grpc.ClientConn{},
		volumeInfoMap:            map[string]*info.VolumeInfo{},
		segmentServerClientMap:   map[string]segpb.SegmentServerClient{},
	}
	return c
}

type controller struct {
	cfg                      *ControllerConfig
	kvStore                  kv.Client
	volumePool               *volumePool
	eventBusMap              map[string]*info.BusInfo
	segmentServerCredentials credentials.TransportCredentials
	segmentServerInfoMap     map[string]*info.SegmentServerInfo
	segmentServerConn        map[string]*grpc.ClientConn
	volumeInfoMap            map[string]*info.VolumeInfo
	segmentServerClientMap   map[string]segpb.SegmentServerClient
	eventLogMgr              *eventlogManager
}

func (ctrl *controller) Start(ctx context.Context) error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvStore = store

	pairs, err := store.List(ctx, eventbusKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		busInfo := &info.BusInfo{}
		err := json.Unmarshal(pair.Value, busInfo)
		if err != nil {
			return err
		}
		ctrl.eventBusMap[filepath.Base(pair.Key)] = busInfo
	}

	if err = ctrl.volumePool.init(ctx, ctrl); err != nil {
		return err
	}

	ctrl.eventLogMgr = newEventlogManager(ctrl)
	if err = ctrl.eventLogMgr.start(ctx); err != nil {
		return err
	}

	if err := ctrl.eventLogMgr.initVolumeInfo(ctrl.volumePool); err != nil {
		return err
	}

	return nil
}

func (ctrl *controller) Stop() error {
	return nil
}

func (ctrl *controller) CreateEventBus(ctx context.Context, req *ctrlpb.CreateEventBusRequest) (*metapb.EventBus, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	if req.LogNumber == 0 {
		req.LogNumber = 1
	}
	elNum := 1 // force set to 1 temporary
	eb := &info.BusInfo{
		ID:        uuid.NewString(), // TODO use another id generator
		Name:      req.Name,
		LogNumber: elNum,
		EventLogs: make([]*info.EventLogInfo, elNum),
	}
	exist, err := ctrl.kvStore.Exists(ctx, eb.Name)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "invoke kv exist failed", err)
	}
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventbus resource name conflicted")
	}
	for idx := 0; idx < eb.LogNumber; idx++ {
		el, err := ctrl.eventLogMgr.acquireEventLog(ctx)
		if err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "binding eventlog failed", err)
		}
		eb.EventLogs[idx] = el
		el.EventBusName = eb.Name
	}
	ctrl.eventBusMap[eb.Name] = eb

	// TODO add rollback handler when persist data to kv failed
	{
		data, _ := json.Marshal(eb)
		if err := ctrl.kvStore.Set(ctx, ctrl.getEventBusKeyInKVStore(eb.Name), data); err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "insert meta to kv store failed", err)
		}
		if err := ctrl.eventLogMgr.updateEventLog(ctx, eb.EventLogs...); err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "update eventlog in kv store failed", err)
		}
	}
	return &metapb.EventBus{
		Name:      eb.Name,
		LogNumber: int32(eb.LogNumber),
		Logs:      info.Convert2ProtoEventLog(eb.EventLogs...),
	}, nil
}

func (ctrl *controller) DeleteEventBus(ctx context.Context,
	eb *metapb.EventBus) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetEventBus(ctx context.Context,
	eb *metapb.EventBus) (*metapb.EventBus, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	_eb, exist := ctrl.eventBusMap[eb.Name]
	if !exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventbus not found")
	}
	return info.Convert2ProtoEventBus(_eb)[0], nil
}

func (ctrl *controller) UpdateEventBus(ctx context.Context,
	req *ctrlpb.UpdateEventBusRequest) (*metapb.EventBus, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &metapb.EventBus{}, nil
}

func (ctrl *controller) ListSegment(ctx context.Context,
	req *ctrlpb.ListSegmentRequest) (*ctrlpb.ListSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	el := ctrl.eventLogMgr.getEventLog(ctx, req.EventLogId)
	if el == nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: info.Convert2ProtoSegment(ctrl.eventLogMgr.getEventLogSegmentList(ctx, el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	serverInfo := &info.SegmentServerInfo{
		Address:           req.Address,
		Volume:            nil,
		StartedAt:         time.Now(),
		LastHeartbeatTime: time.Now(),
	}

	volumeInfo, exist := ctrl.volumeInfoMap[req.VolumeId]
	if !exist {
		volumeInfo = ctrl.volumePool.get(req.VolumeId)
		if volumeInfo == nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
				"invalid volumeID, PVC not found", nil)
		}
	}
	// TODO clean server info when server disconnected
	_, exist = ctrl.segmentServerInfoMap[serverInfo.Address]
	if exist {
		if volumeInfo.GetAccessEndpoint() == "" {
			log.Info("the segment server reconnected", map[string]interface{}{
				"server_address": req.Address,
				"volume_id":      req.VolumeId,
			})
		} else if volumeInfo.GetAccessEndpoint() != volumeInfo.GetAccessEndpoint() {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
				"segpb server ip address is conflicted", nil)
		}
	}

	if err := ctrl.volumePool.ActivateVolume(volumeInfo, serverInfo); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"bind volume to segment server failed", err)
	}
	ctrl.segmentServerInfoMap[serverInfo.Address] = serverInfo
	// TODO update state in KV store
	go ctrl.readyToStartSegmentServer(context.Background(), serverInfo)
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
	ctrl.volumePool.InactivateVolume(serverInfo.Volume)
	return &ctrlpb.UnregisterSegmentServerResponse{}, nil
}

func (ctrl *controller) QuerySegmentRouteInfo(ctx context.Context,
	req *ctrlpb.QuerySegmentRouteInfoRequest) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
	return &ctrlpb.QuerySegmentRouteInfoResponse{}, nil
}

func (ctrl *controller) SegmentHeartbeat(srv ctrlpb.SegmentController_SegmentHeartbeatServer) error {
	var err error
	var req *ctrlpb.SegmentHeartbeatRequest
	for {
		req, err = srv.Recv()
		if err != nil {
			break
		}
		t, err := util.ParseTime(req.ReportTime)
		if err != nil {
			log.Error("parse heartbeat report time failed", map[string]interface{}{
				"volume_id":  req.VolumeId,
				"server_id":  req.ServerId,
				log.KeyError: err,
			})
			continue
		}
		log.Debug("received heartbeat from segment server", map[string]interface{}{
			"server_id": req.ServerId,
			"volume_id": req.VolumeId,
			"time":      t,
		})
		vInfo := ctrl.volumePool.get(req.VolumeId)
		if vInfo == nil {
			log.Error("received a heartbeat request, but volume not found", map[string]interface{}{
				"volume_id": req.VolumeId,
				"server_id": req.ServerId,
			})
			continue
		}
		serverInfo := ctrl.segmentServerInfoMap[req.ServerAddr]
		if serverInfo == nil {
			// TODO refactor here and register
			serverInfo = &info.SegmentServerInfo{
				Address:   req.ServerAddr,
				Volume:    vInfo,
				StartedAt: time.Now(),
			}
			ctrl.segmentServerInfoMap[req.ServerAddr] = serverInfo
			//log.Error("received a heartbeat request, but server info not found", map[string]interface{}{
			//	"volume_id": req.VolumeId,
			//	"server_id": req.ServerId,
			//})
		}

		if !vInfo.IsActivity() || !vInfo.IsOnline() {
			ctrl.volumePool.ActivateVolume(vInfo, serverInfo)
			log.Info("the volume has been activated", map[string]interface{}{
				"volume_id":   vInfo.ID(),
				"server_addr": serverInfo.Address,
			})
		}

		// use local time to avoiding time shift
		serverInfo.LastHeartbeatTime = time.Now()
		if _err := ctrl.eventLogMgr.updateSegment(context.Background(), req); _err != nil {
			log.Warning("update segment when received segment server heartbeat", map[string]interface{}{
				log.KeyError: err,
				"volume_id":  req.VolumeId,
				"server_id":  req.ServerId,
			})
		}
		// TODO srv.SendAndClose()
	}

	if err != nil && err != io.EOF {
		log.Error("segment server heartbeat error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return nil
}

func (ctrl *controller) GetAppendableSegment(ctx context.Context,
	req *ctrlpb.GetAppendableSegmentRequest) (*ctrlpb.GetAppendableSegmentResponse, error) {
	eli := ctrl.eventLogMgr.getEventLog(ctx, req.EventLogId)
	if eli == nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventlog not found")
	}
	num := int(req.Limited)
	if num == 0 {
		num = 1
	}
	segInfos, err := ctrl.eventLogMgr.getAppendableSegment(ctx, eli, num)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "get segment error")
	}
	segs := make([]*metapb.Segment, 0)
	for idx := 0; idx < len(segInfos); idx++ {
		seg := segInfos[idx]
		segs = append(segs, info.Convert2ProtoSegment(seg)...)
	}
	return &ctrlpb.GetAppendableSegmentResponse{Segments: segs}, nil
}

func (ctrl *controller) ReportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, ctrl.eventLogMgr.updateSegment(ctx, req)
}

func (ctrl *controller) getSegmentServerClient(i *info.SegmentServerInfo) segpb.SegmentServerClient {
	if i == nil {
		return nil
	}
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
		cli = segpb.NewSegmentServerClient(conn)
		ctrl.segmentServerClientMap[i.ID()] = cli
	}
	return cli
}

func (ctrl *controller) readyToStartSegmentServer(ctx context.Context, serverInfo *info.SegmentServerInfo) {
	conn := ctrl.getSegmentServerClient(serverInfo)
	if conn == nil {
		return
	}
	_, err := conn.Start(ctx, &segpb.StartSegmentServerRequest{
		SegmentServerId: uuid.NewString(),
	})
	if err != nil {
		log.Warning("start segment server failed", map[string]interface{}{
			log.KeyError: err,
			"address":    serverInfo.Address,
		})
	}
}

func (ctrl *controller) getEventBusKeyInKVStore(ebName string) string {
	return strings.Join([]string{eventbusKeyPrefixInKVStore, ebName}, "/")
}
