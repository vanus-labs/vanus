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
	stdErr "errors"
	"io"
	"path/filepath"
	"sync"
	"time"

	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/eventlog"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/pkg/util"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ ctrlpb.EventBusControllerServer = &controller{}
	_ ctrlpb.EventLogControllerServer = &controller{}
	_ ctrlpb.SegmentControllerServer  = &controller{}
	_ ctrlpb.PingServerServer         = &controller{}
)

func NewController(cfg Config, member embedetcd.Member) *controller {
	c := &controller{
		cfg:         &cfg,
		ssMgr:       server.NewServerManager(),
		eventBusMap: map[string]*metadata.Eventbus{},
		member:      member,
		isLeader:    false,
		readyNotify: make(chan error, 1),
		stopNotify:  make(chan error, 1),
	}
	c.volumeMgr = volume.NewVolumeManager(c.ssMgr)
	c.eventLogMgr = eventlog.NewManager(c.volumeMgr, cfg.Replicas, cfg.SegmentCapacity)
	return c
}

type controller struct {
	cfg             *Config
	kvStore         kv.Client
	volumeMgr       volume.Manager
	eventLogMgr     eventlog.Manager
	ssMgr           server.Manager
	eventBusMap     map[string]*metadata.Eventbus
	member          embedetcd.Member
	cancelCtx       context.Context
	cancelFunc      context.CancelFunc
	membershipMutex sync.Mutex
	isLeader        bool
	readyNotify     chan error
	stopNotify      chan error
	mutex           sync.Mutex
}

func (ctrl *controller) Start(_ context.Context) error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvStore = store

	ctrl.cancelCtx, ctrl.cancelFunc = context.WithCancel(context.Background())
	go ctrl.member.RegisterMembershipChangedProcessor(ctrl.membershipChangedProcessor)
	return nil
}

func (ctrl *controller) Stop() {
	ctrl.stop(context.Background(), nil)
}

func (ctrl *controller) ReadyNotify() <-chan error {
	return ctrl.readyNotify
}

func (ctrl *controller) StopNotify() <-chan error {
	return ctrl.stopNotify
}

func (ctrl *controller) CreateEventBus(ctx context.Context,
	req *ctrlpb.CreateEventBusRequest) (*metapb.EventBus, error) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	if req.LogNumber == 0 {
		req.LogNumber = 1
	}
	elNum := 1 // force set to 1 temporary
	eb := &metadata.Eventbus{
		ID:        vanus.NewID(),
		Name:      req.Name,
		LogNumber: elNum,
		EventLogs: make([]*metadata.Eventlog, elNum),
	}
	exist, err := ctrl.kvStore.Exists(ctx, metadata.GetEventbusMetadataKey(eb.Name))
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("the eventbus already exist")
	}
	for idx := 0; idx < eb.LogNumber; idx++ {
		el, err := ctrl.eventLogMgr.AcquireEventLog(ctx, eb.ID)
		if err != nil {
			return nil, err
		}
		eb.EventLogs[idx] = el
	}
	ctrl.eventBusMap[eb.Name] = eb

	{
		data, _ := json.Marshal(eb)
		if err := ctrl.kvStore.Set(ctx, metadata.GetEventbusMetadataKey(eb.Name), data); err != nil {
			return nil, err
		}
	}
	metrics.EventbusGauge.Set(float64(len(ctrl.eventBusMap)))
	return ctrl.getEventbus(eb.Name)
}

func (ctrl *controller) DeleteEventBus(ctx context.Context, eb *metapb.EventBus) (*emptypb.Empty, error) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()

	bus, exist := ctrl.eventBusMap[eb.Name]
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the eventbus doesn't exist")
	}
	err := ctrl.kvStore.Delete(ctx, metadata.GetEventbusMetadataKey(eb.Name))
	if err != nil {
		return nil, errors.ErrInternal.WithMessage("delete eventbus metadata in kv failed").Wrap(err)
	}

	// TODO(wenfeng.wang) notify gateway to cut flow
	delete(ctrl.eventBusMap, eb.Name)
	wg := sync.WaitGroup{}

	for _, v := range bus.EventLogs {
		wg.Add(1)
		go func(logID vanus.ID) {
			ctrl.eventLogMgr.DeleteEventlog(ctx, logID)
			wg.Done()
		}(v.ID)
	}
	wg.Wait()
	metrics.EventbusGauge.Set(float64(len(ctrl.eventBusMap)))
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetEventBus(ctx context.Context, eb *metapb.EventBus) (*metapb.EventBus, error) {
	return ctrl.getEventbus(eb.Name)
}

func (ctrl *controller) getEventbus(name string) (*metapb.EventBus, error) {
	_eb, exist := ctrl.eventBusMap[name]
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
	}

	ebMD := metadata.Convert2ProtoEventBus(_eb)[0]
	ebMD.Name = _eb.Name
	ebMD.Logs = metadata.Convert2ProtoEventLog(_eb.EventLogs...)
	addrs := make([]string, 0)
	for _, v := range ctrl.cfg.Topology {
		addrs = append(addrs, v)
	}
	for _, v := range ebMD.Logs {
		v.EventBusName = ebMD.Name
		v.ServerAddress = addrs
	}
	return ebMD, nil
}

func (ctrl *controller) ListEventBus(ctx context.Context, _ *emptypb.Empty) (*ctrlpb.ListEventbusResponse, error) {
	eventbusList := make([]*metapb.EventBus, 0)
	for _, v := range ctrl.eventBusMap {
		ebMD := metadata.Convert2ProtoEventBus(v)[0]
		eventbusList = append(eventbusList, ebMD)
	}
	return &ctrlpb.ListEventbusResponse{Eventbus: eventbusList}, nil
}

func (ctrl *controller) UpdateEventBus(ctx context.Context,
	req *ctrlpb.UpdateEventBusRequest) (*metapb.EventBus, error) {
	return &metapb.EventBus{}, nil
}

func (ctrl *controller) ListSegment(ctx context.Context,
	req *ctrlpb.ListSegmentRequest) (*ctrlpb.ListSegmentResponse, error) {
	el := ctrl.eventLogMgr.GetEventLog(ctx, vanus.NewIDFromUint64(req.EventLogId))
	if el == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: eventlog.Convert2ProtoSegment(ctx, ctrl.eventLogMgr.GetEventLogSegmentList(el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	srv, err := server.NewSegmentServer(req.Address)
	if err != nil {
		return nil, err
	}

	if err = ctrl.ssMgr.AddServer(ctx, srv); err != nil {
		return nil, err
	}

	var volInstance server.Instance
	// need to compare metadata if existed?
	volInstance = ctrl.volumeMgr.GetVolumeInstanceByID(vanus.NewIDFromUint64(req.VolumeId))
	if volInstance == nil {
		volMD := &metadata.VolumeMetadata{
			ID:     vanus.NewIDFromUint64(req.VolumeId),
			Blocks: map[uint64]*metadata.Block{},
		}
		_volInstance, err := ctrl.volumeMgr.RegisterVolume(ctx, volMD)
		if err != nil {
			return nil, err
		}
		volInstance = _volInstance
	}

	segments := make(map[uint64]*metapb.Segment)
	blocks, err := ctrl.volumeMgr.GetBlocksOfVolume(ctx, volInstance)
	if err != nil {
		return nil, err
	}
	for _, v := range blocks {
		if v.EventlogID == vanus.EmptyID() {
			continue
		}
		seg, err := ctrl.eventLogMgr.GetSegmentByBlockID(v)
		if err != nil {
			return nil, err
		}
		segments[seg.ID.Uint64()] = eventlog.Convert2ProtoSegment(ctx, seg)[0]
	}

	go func() {
		newCtx := context.Background()
		if err := srv.RemoteStart(newCtx); err == nil {
			ctrl.volumeMgr.UpdateRouting(newCtx, volInstance, srv)
		}
	}()

	return &ctrlpb.RegisterSegmentServerResponse{
		ServerId: srv.ID().Uint64(),
		Segments: segments,
	}, nil
}

func (ctrl *controller) UnregisterSegmentServer(ctx context.Context,
	req *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	srv := ctrl.ssMgr.GetServerByAddress(req.Address)

	if srv == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("block server not found")
	}

	if err := ctrl.ssMgr.RemoveServer(ctx, srv); err != nil {
		log.Warning(ctx, "remove server from segmentServerManager error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	volIns := ctrl.volumeMgr.GetVolumeInstanceByID(vanus.NewIDFromUint64(req.VolumeId))
	if volIns == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("volume instance not found")
	}
	ctrl.volumeMgr.UpdateRouting(ctx, volIns, nil)
	return &ctrlpb.UnregisterSegmentServerResponse{}, nil
}

func (ctrl *controller) QuerySegmentRouteInfo(ctx context.Context,
	req *ctrlpb.QuerySegmentRouteInfoRequest) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
	return &ctrlpb.QuerySegmentRouteInfoResponse{}, nil
}

func (ctrl *controller) SegmentHeartbeat(srv ctrlpb.SegmentController_SegmentHeartbeatServer) error {
	var err error
	var req *ctrlpb.SegmentHeartbeatRequest
	ctx := context.Background()
	for {
		select {
		case <-ctrl.cancelCtx.Done():
			log.Info(ctx, "exit to heartbeat processing due to server stopped", nil)
			_ = srv.SendAndClose(&ctrlpb.SegmentHeartbeatResponse{})
			return nil
		default:
		}
		req, err = srv.Recv()
		if err != nil {
			break
		}
		if !ctrl.member.IsLeader() {
			err = srv.SendAndClose(&ctrlpb.SegmentHeartbeatResponse{})
			break
		}

		_ = ctrl.processHeartbeat(ctx, req)
	}

	if err != nil && stdErr.Is(err, io.EOF) {
		sts := status.Convert(err)
		if sts != nil && sts.Code() != codes.Canceled {
			log.Warning(ctx, "block server heartbeat error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}
	return nil
}
func (ctrl *controller) processHeartbeat(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	if !ctrl.member.IsLeader() {
		return errors.ErrNotLeader
	}

	t, err := util.ParseTime(req.ReportTime)
	if err != nil {
		log.Error(ctx, "parse heartbeat report time failed", map[string]interface{}{
			"volume_id":  req.VolumeId,
			"server_id":  req.ServerId,
			log.KeyError: err,
		})
		return err
	}
	log.Debug(ctx, "received heartbeat from segment server", map[string]interface{}{
		"server_id": req.ServerId,
		"volume_id": req.VolumeId,
		"time":      t,
	})

	srv := ctrl.ssMgr.GetServerByServerID(vanus.NewIDFromUint64(req.ServerId))
	if srv == nil {
		log.Warning(ctx, "received a heartbeat request, but server metadata not found", map[string]interface{}{
			"volume_id": req.VolumeId,
			"server_id": req.ServerId,
		})
	} else {
		srv.Polish()
	}
	segments := make(map[string][]eventlog.Segment)
	for _, info := range req.HealthInfo {
		blockID := vanus.NewIDFromUint64(info.Id)
		block := ctrl.eventLogMgr.GetBlock(blockID)
		if block == nil {
			continue
		}
		if info.Size == 0 {
			continue
		}
		logArr, exist := segments[block.EventlogID.Key()]
		if !exist {
			logArr = make([]eventlog.Segment, 0)
			segments[block.EventlogID.Key()] = logArr
		}

		seg := eventlog.Segment{
			ID:                 block.SegmentID,
			Capacity:           info.Capacity,
			EventLogID:         block.EventlogID,
			Size:               info.Size,
			Number:             info.EventNumber,
			FirstEventBornTime: time.UnixMilli(info.FirstEventBornTime),
			LastEventBornTime:  time.UnixMilli(info.LastEventBornTime),
		}
		if info.IsFull {
			seg.State = eventlog.StateFrozen
		}
		logArr = append(logArr, seg)
		segments[block.EventlogID.Key()] = logArr
	}
	ctrl.eventLogMgr.UpdateSegment(ctx, segments)
	return nil
}

func (ctrl *controller) GetAppendableSegment(ctx context.Context,
	req *ctrlpb.GetAppendableSegmentRequest) (*ctrlpb.GetAppendableSegmentResponse, error) {
	eli := ctrl.eventLogMgr.GetEventLog(ctx, vanus.NewIDFromUint64(req.EventLogId))
	if eli == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}
	num := int(req.Limited)
	if num == 0 {
		num = 1
	}
	segInfos, err := ctrl.eventLogMgr.GetAppendableSegment(ctx, eli, num)
	if err != nil {
		return nil, err
	}
	return &ctrlpb.GetAppendableSegmentResponse{Segments: eventlog.Convert2ProtoSegment(ctx, segInfos...)}, nil
}

func (ctrl *controller) ReportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	if err := ctrl.processHeartbeat(ctx, req); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) Ping(_ context.Context, _ *emptypb.Empty) (*ctrlpb.PingResponse, error) {
	return &ctrlpb.PingResponse{
		LeaderAddr: ctrl.member.GetLeaderAddr(),
	}, nil
}

func (ctrl *controller) ReportSegmentLeader(ctx context.Context,
	req *ctrlpb.ReportSegmentLeaderRequest) (*emptypb.Empty, error) {
	err := ctrl.eventLogMgr.UpdateSegmentReplicas(ctx, vanus.NewIDFromUint64(req.LeaderId), req.Term)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) membershipChangedProcessor(ctx context.Context, event embedetcd.MembershipChangedEvent) error {
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()

	switch event.Type {
	case embedetcd.EventBecomeLeader:
		if ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = true
		if err := ctrl.loadEventbus(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.volumeMgr.Init(ctx, ctrl.kvStore); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.eventLogMgr.Run(ctx, ctrl.kvStore, true); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.ssMgr.Run(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}
	case embedetcd.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = false
		ctrl.eventLogMgr.Stop()
		ctrl.ssMgr.Stop(ctx)
	}
	return nil
}

func (ctrl *controller) loadEventbus(ctx context.Context) error {
	// load eventbus metadata
	pairs, err := ctrl.kvStore.List(ctx, metadata.EventbusKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		busInfo := &metadata.Eventbus{}
		err := json.Unmarshal(pair.Value, busInfo)
		if err != nil {
			return err
		}
		ctrl.eventBusMap[filepath.Base(pair.Key)] = busInfo
	}
	metrics.EventbusGauge.Set(float64(len(ctrl.eventBusMap)))
	return nil
}

func (ctrl *controller) stop(ctx context.Context, err error) {
	ctrl.member.ResignIfLeader()
	ctrl.cancelFunc()
	ctrl.stopNotify <- err
	if err := ctrl.kvStore.Close(); err != nil {
		log.Warning(ctx, "close kv client error", map[string]interface{}{
			log.KeyError: err,
		})
		ctrl.stopNotify <- err
	}
	close(ctrl.readyNotify)
	close(ctrl.stopNotify)
}
