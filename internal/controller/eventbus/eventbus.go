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
	"fmt"
	"github.com/google/uuid"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
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
	"strings"
	"sync"
	"time"
)

func NewEventBusController(cfg ControllerConfig) *controller {
	c := &controller{
		cfg: &cfg,
		segmentPool: &segmentPool{
			eventLogSegment: map[string]*skiplist.SkipList{},
		},
		volumePool:               &volumePool{},
		eventBusMap:              map[string]*info.BusInfo{},
		eventLogInfoMap:          map[string]*info.EventLogInfo{},
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
	segmentPool              *segmentPool
	volumePool               *volumePool
	eventBusMap              map[string]*info.BusInfo
	eventLogInfoMap          map[string]*info.EventLogInfo
	segmentServerCredentials credentials.TransportCredentials
	segmentServerInfoMap     map[string]*info.SegmentServerInfo
	segmentServerConn        map[string]*grpc.ClientConn
	volumeInfoMap            map[string]*info.VolumeInfo
	segmentServerClientMap   map[string]segpb.SegmentServerClient
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
	if err = ctrl.volumePool.init(ctrl); err != nil {
		return err
	}
	return ctrl.dynamicScaleUpEventLog()
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
	elNum := 1 // temporary
	eb := &info.BusInfo{
		Namespace: req.Namespace,
		Name:      req.Name,
		LogNumber: elNum, //req.LogNumber, force set to 1 temporary
		EventLogs: make([]*info.EventLogInfo, elNum),
	}
	exist, err := ctrl.kvStore.Exists(eb.Name)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "invoke kv exist failed", err)
	}
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventbus resource name conflicted")
	}
	wg := sync.WaitGroup{}
	for idx := 0; idx < eb.LogNumber; idx++ {
		eb.EventLogs[idx] = &info.EventLogInfo{
			ID:           fmt.Sprintf("%s-%d", eb.Name, idx),
			EventBusName: eb.Name,
		}
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

	for idx := 0; idx < len(eb.EventLogs); idx++ {
		ctrl.eventLogInfoMap[eb.EventLogs[idx].ID] = eb.EventLogs[idx]
	}

	//data, _ := json.Marshal(eb)
	//ctrl.kvStore.Set(eb.VRN.Value, data)
	// TODO reconsider valuable of VRN, it maybe cause some chaos
	ctrl.eventBusMap[eb.Name] = eb
	return &metapb.EventBus{
		Namespace: eb.Namespace,
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

	el, exist := ctrl.eventLogInfoMap[req.EventLogId]
	if !exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: info.Convert2ProtoSegment(ctrl.segmentPool.getEventLogSegmentList(el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	serverInfo := &info.SegmentServerInfo{}
	serverInfo.Address = req.Address
	_, exist := ctrl.segmentServerInfoMap[serverInfo.ID()]
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"segpb server ip address is conflicted", nil)
	}
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
			"bind volume to segpb server failed", err)
	}
	serverInfo.Volume = volumeInfo
	ctrl.segmentServerInfoMap[serverInfo.ID()] = serverInfo
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
			"segpb server not found", nil)
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
	var err error
	var req *ctrlpb.SegmentHeartbeatRequest
	for {
		req, err = srv.Recv()
		if err != nil {
			break
		}
		log.Debug("received heartbeat from segment server", map[string]interface{}{
			"server_id": req.ServerId,
			"volume_id": req.VolumeId,
			"time":      time.Unix(req.ReportTimeInNano/10e9, req.ReportTimeInNano-req.ReportTimeInNano/10e9),
		})
		if _err := ctrl.segmentPool.updateSegment(context.Background(), req); _err != nil {
			log.Warning("update segment when received segment server heartbeat", map[string]interface{}{
				log.KeyError: err,
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
	eli := ctrl.eventLogInfoMap[req.EventLogId]
	if eli == nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventlog not found")
	}
	num := int(req.Limited)
	if num == 0 {
		num = 1
	}
	segInfos, err := ctrl.segmentPool.getAppendableSegment(ctx, eli, num)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "get segment error")
	}
	segs := make([]*metapb.Segment, 0)
	for idx := 0; idx < len(segInfos); idx++ {
		seg := segInfos[idx]
		segs = append(segs, &metapb.Segment{
			Id:                seg.ID,
			PreviousSegmentId: seg.PreviousSegmentId,
			NextSegmentId:     seg.NextSegmentId,
			EventLogId:        seg.EventLogID,
			Tier:              metapb.StorageTier_SSD,
			StorageUri:        seg.VolumeInfo.AssignedSegmentServer.Address,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number),
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Compressed:        metapb.CompressAlgorithm_NONE,
		})
	}
	return &ctrlpb.GetAppendableSegmentResponse{Segments: segs}, nil
}

func (ctrl *controller) ReportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, ctrl.segmentPool.updateSegment(ctx, req)
}

func (ctrl *controller) initializeEventLog(ctx context.Context, el *info.EventLogInfo) error {
	// TODO eliminate magic number
	_, err := ctrl.segmentPool.bindSegment(ctx, el, 3)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *controller) dynamicScaleUpEventLog() error {
	return nil
}

func (ctrl *controller) generateEventBusVRN(eb *info.BusInfo) *metapb.VanusResourceName {
	return &metapb.VanusResourceName{
		Value: strings.Join([]string{"eventbus", eb.Namespace, eb.Name}, ":"),
	}
}

func (ctrl *controller) generateEventLogVRN(el *info.EventLogInfo) *metapb.VanusResourceName {
	return &metapb.VanusResourceName{
		Value: strings.Join([]string{el.EventBusName, "eventlog", fmt.Sprintf("%d", el.ID)}, ":"),
	}
}

func (ctrl *controller) getSegmentServerClient(i *info.SegmentServerInfo) segpb.SegmentServerClient {
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
