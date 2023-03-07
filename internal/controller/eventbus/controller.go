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
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/pkg/util"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"

	"github.com/vanus-labs/vanus/internal/controller/eventbus/eventlog"
	"github.com/vanus-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/vanus-labs/vanus/internal/controller/eventbus/server"
	"github.com/vanus-labs/vanus/internal/controller/eventbus/volume"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/kv/etcd"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

var (
	_ ctrlpb.EventbusControllerServer = &controller{}
	_ ctrlpb.EventlogControllerServer = &controller{}
	_ ctrlpb.SegmentControllerServer  = &controller{}
	_ ctrlpb.PingServerServer         = &controller{}
)

const (
	maximumEventlogNum = 64
)

func NewController(cfg Config, mem member.Member) *controller {
	c := &controller{
		cfg:         &cfg,
		ssMgr:       server.NewServerManager(),
		eventbusMap: map[string]*metadata.Eventbus{},
		member:      mem,
		isLeader:    false,
		readyNotify: make(chan error, 1),
		stopNotify:  make(chan error, 1),
	}
	c.volumeMgr = volume.NewVolumeManager(c.ssMgr)
	c.eventlogMgr = eventlog.NewManager(c.volumeMgr, cfg.Replicas, cfg.SegmentCapacity)
	return c
}

type controller struct {
	cfg                  *Config
	kvStore              kv.Client
	volumeMgr            volume.Manager
	eventlogMgr          eventlog.Manager
	ssMgr                server.Manager
	eventbusMap          map[string]*metadata.Eventbus
	member               member.Member
	cancelCtx            context.Context
	cancelFunc           context.CancelFunc
	membershipMutex      sync.Mutex
	isLeader             bool
	readyNotify          chan error
	stopNotify           chan error
	mutex                sync.Mutex
	eventbusUpdatedCount int64
	eventbusDeletedCount int64
}

func (ctrl *controller) Start(_ context.Context) error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvStore = store
	ctrl.cancelCtx, ctrl.cancelFunc = context.WithCancel(context.Background())
	go ctrl.member.RegisterMembershipChangedProcessor(ctrl.membershipChangedProcessor)
	go ctrl.recordMetrics()
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

func (ctrl *controller) CreateEventbus(
	ctx context.Context, req *ctrlpb.CreateEventbusRequest,
) (*metapb.Eventbus, error) {
	if err := isValidEventbusName(req.Name); err != nil {
		return nil, err
	}
	eb, err := ctrl.createEventbus(ctx, req)
	if err != nil {
		return nil, err
	}
	// TODO async create
	// create dead letter eventbus
	_, err = ctrl.createEventbus(context.Background(), &ctrlpb.CreateEventbusRequest{
		Name:        primitive.GetDeadLetterEventbusName(req.Name),
		LogNumber:   1,
		Description: "System DeadLetter Eventbus For " + req.Name,
	})
	if err != nil {
		log.Error(context.Background(), "create dead letter eventbus error", map[string]interface{}{
			log.KeyError:        err,
			log.KeyEventbusName: req.Name,
		})
	}
	return eb, nil
}

func isValidEventbusName(name string) error {
	name = strings.ToLower(name)
	for _, v := range name {
		if v == '.' || v == '_' || v == '-' {
			continue
		}
		c := v - 'a'
		if c >= 0 || c <= 26 {
			continue
		} else {
			c = v - '0'
			if c >= 0 || c <= 9 {
				continue
			}
			return errors.ErrInvalidRequest.WithMessage(
				"eventbus name must be insist of 0-9a-zA-Z.-_")
		}
	}
	return nil
}

func (ctrl *controller) CreateSystemEventbus(
	ctx context.Context, req *ctrlpb.CreateEventbusRequest,
) (*metapb.Eventbus, error) {
	if !strings.HasPrefix(req.Name, primitive.SystemEventbusNamePrefix) {
		return nil, errors.ErrInvalidRequest.WithMessage("system eventbus must start with __")
	}
	return ctrl.createEventbus(ctx, req)
}

func (ctrl *controller) createEventbus(
	ctx context.Context, req *ctrlpb.CreateEventbusRequest,
) (*metapb.Eventbus, error) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	if !ctrl.isReady(ctx) {
		return nil, errors.ErrResourceCanNotOp.WithMessage(
			"the cluster isn't ready for create eventbus")
	}
	logNum := req.LogNumber
	if logNum == 0 {
		logNum = 1
	}
	if logNum > maximumEventlogNum {
		return nil, errors.ErrInvalidRequest.WithMessage(fmt.Sprintf("the number of eventlog exceeded,"+
			" maximum is %d", maximumEventlogNum))
	}

	id, err := vanus.NewID()
	if err != nil {
		log.Warning(ctx, "failed to create eventbus ID", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}
	eb := &metadata.Eventbus{
		ID:          id,
		Name:        req.Name,
		LogNumber:   int(logNum),
		Eventlogs:   make([]*metadata.Eventlog, int(logNum)),
		Description: req.Description,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	exist, err := ctrl.kvStore.Exists(ctx, metadata.GetEventbusMetadataKey(eb.Name))
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("the eventbus already exist")
	}
	for idx := 0; idx < eb.LogNumber; idx++ {
		el, err := ctrl.eventlogMgr.AcquireEventlog(ctx, eb.ID, eb.Name)
		if err != nil {
			return nil, err
		}
		eb.Eventlogs[idx] = el
	}
	ctrl.eventbusMap[eb.Name] = eb

	{
		data, _ := json.Marshal(eb)
		if err := ctrl.kvStore.Set(ctx, metadata.GetEventbusMetadataKey(eb.Name), data); err != nil {
			return nil, err
		}
	}
	return ctrl.getEventbus(eb.Name)
}

func (ctrl *controller) DeleteEventbus(ctx context.Context, eb *metapb.Eventbus) (*emptypb.Empty, error) {
	ctrl.mutex.Lock()
	defer ctrl.mutex.Unlock()
	err := ctrl.deleteEventbus(ctx, eb.Name)
	if err != nil {
		return nil, err
	}
	// TODO async delete
	// delete dead letter eventbus
	err = ctrl.deleteEventbus(context.Background(),
		primitive.GetDeadLetterEventbusName(eb.Name))
	if err != nil {
		log.Error(context.Background(), "delete dead letter eventbus error", map[string]interface{}{
			log.KeyError:        err,
			log.KeyEventbusName: eb.Name,
		})
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) deleteEventbus(ctx context.Context, name string) error {
	bus, exist := ctrl.eventbusMap[name]
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("the eventbus doesn't exist")
	}
	err := ctrl.kvStore.Delete(ctx, metadata.GetEventbusMetadataKey(name))
	if err != nil {
		return errors.ErrInternal.WithMessage("delete eventbus metadata in kv failed").Wrap(err)
	}

	// TODO(wenfeng.wang) notify gateway to cut flow
	delete(ctrl.eventbusMap, name)
	wg := sync.WaitGroup{}

	for _, v := range bus.Eventlogs {
		wg.Add(1)
		go func(logID vanus.ID) {
			ctrl.eventlogMgr.DeleteEventlog(ctx, logID)
			wg.Done()
		}(v.ID)
	}
	wg.Wait()
	atomic.AddInt64(&ctrl.eventbusDeletedCount, 1)
	return nil
}

func (ctrl *controller) GetEventbus(ctx context.Context, eb *metapb.Eventbus) (*metapb.Eventbus, error) {
	return ctrl.getEventbus(eb.Name)
}

func (ctrl *controller) getEventbus(name string) (*metapb.Eventbus, error) {
	_eb, exist := ctrl.eventbusMap[name]
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
	}

	ebMD := metadata.Convert2ProtoEventbus(_eb)[0]
	addrs := make([]string, 0)
	for _, v := range ctrl.cfg.Topology {
		addrs = append(addrs, v)
	}
	for _, v := range ebMD.Logs {
		v.EventbusName = ebMD.Name
		v.ServerAddress = addrs
	}
	return ebMD, nil
}

func (ctrl *controller) ListEventbus(ctx context.Context, _ *emptypb.Empty) (*ctrlpb.ListEventbusResponse, error) {
	eventbusList := make([]*metapb.Eventbus, 0)
	for _, v := range ctrl.eventbusMap {
		if strings.HasPrefix(v.Name, primitive.SystemEventbusNamePrefix) {
			continue
		}
		ebMD := metadata.Convert2ProtoEventbus(v)[0]
		eventbusList = append(eventbusList, ebMD)
	}
	return &ctrlpb.ListEventbusResponse{Eventbus: eventbusList}, nil
}

func (ctrl *controller) UpdateEventbus(
	ctx context.Context, req *ctrlpb.UpdateEventbusRequest,
) (*metapb.Eventbus, error) {
	atomic.AddInt64(&ctrl.eventbusUpdatedCount, 1)
	return &metapb.Eventbus{}, nil
}

func (ctrl *controller) ListSegment(
	ctx context.Context, req *ctrlpb.ListSegmentRequest,
) (*ctrlpb.ListSegmentResponse, error) {
	el := ctrl.eventlogMgr.GetEventlog(ctx, vanus.NewIDFromUint64(req.EventlogId))
	if el == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: eventlog.Convert2ProtoSegment(ctx,
			ctrl.eventlogMgr.GetEventlogSegmentList(el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(
	ctx context.Context, req *ctrlpb.RegisterSegmentServerRequest,
) (*ctrlpb.RegisterSegmentServerResponse, error) {
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
		seg, err := ctrl.eventlogMgr.GetSegmentByBlockID(v)
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
	req *ctrlpb.UnregisterSegmentServerRequest,
) (*ctrlpb.UnregisterSegmentServerResponse, error) {
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
	req *ctrlpb.QuerySegmentRouteInfoRequest,
) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
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
		block := ctrl.eventlogMgr.GetBlock(blockID)
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
			EventlogID:         block.EventlogID,
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
	ctrl.eventlogMgr.UpdateSegment(ctx, segments)
	return nil
}

func (ctrl *controller) GetAppendableSegment(
	ctx context.Context, req *ctrlpb.GetAppendableSegmentRequest,
) (*ctrlpb.GetAppendableSegmentResponse, error) {
	eli := ctrl.eventlogMgr.GetEventlog(ctx, vanus.NewIDFromUint64(req.EventlogId))
	if eli == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}
	num := int(req.Limited)
	if num == 0 {
		num = 1
	}
	segInfos, err := ctrl.eventlogMgr.GetAppendableSegment(ctx, eli, num)
	if err != nil {
		return nil, err
	}
	return &ctrlpb.GetAppendableSegmentResponse{Segments: eventlog.Convert2ProtoSegment(ctx, segInfos...)}, nil
}

func (ctrl *controller) ReportSegmentBlockIsFull(
	ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest,
) (*emptypb.Empty, error) {
	for _, info := range req.GetHealthInfo() {
		log.Info(ctx, "Received segment block is full report.", map[string]interface{}{
			"block_id":   vanus.NewIDFromUint64(info.GetId()),
			"event_num":  info.GetEventNumber(),
			"event_size": info.GetSize(),
		})
	}
	if err := ctrl.processHeartbeat(ctx, req); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) Ping(ctx context.Context, _ *emptypb.Empty) (*ctrlpb.PingResponse, error) {
	return &ctrlpb.PingResponse{
		LeaderAddr:      ctrl.member.GetLeaderAddr(),
		IsEventbusReady: ctrl.isReady(ctx),
	}, nil
}

func (ctrl *controller) isReady(ctx context.Context) bool {
	if ctrl.member == nil {
		return false
	}
	if !ctrl.member.IsLeader() && !ctrl.member.IsReady() || ctrl.member.GetLeaderAddr() == "" {
		return false
	}
	return ctrl.ssMgr.CanCreateEventbus(ctx, int(ctrl.cfg.Replicas))
}

func (ctrl *controller) ReportSegmentLeader(
	ctx context.Context, req *ctrlpb.ReportSegmentLeaderRequest,
) (*emptypb.Empty, error) {
	err := ctrl.eventlogMgr.UpdateSegmentReplicas(ctx, vanus.NewIDFromUint64(req.LeaderId), req.Term)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) recordMetrics() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			ctrl.membershipMutex.Lock()
			if ctrl.isLeader {
				metrics.ControllerLeaderGaugeVec.DeleteLabelValues(strconv.FormatBool(!ctrl.isLeader))
				metrics.ControllerLeaderGaugeVec.WithLabelValues(
					strconv.FormatBool(ctrl.isLeader)).Set(1)
			} else {
				metrics.ControllerLeaderGaugeVec.WithLabelValues(
					strconv.FormatBool(!ctrl.isLeader)).Set(0)
			}
			ctrl.membershipMutex.Unlock()

			ctrl.mutex.Lock()
			metrics.EventbusGauge.Set(float64(len(ctrl.eventbusMap)))
			metrics.EventbusUpdatedGauge.Set(float64(
				atomic.LoadInt64(&ctrl.eventbusUpdatedCount)))
			metrics.EventbusDeletedGauge.Set(float64(
				atomic.LoadInt64(&ctrl.eventbusDeletedCount)))
			ctrl.mutex.Unlock()
		case <-ctrl.cancelCtx.Done():
			log.Info(ctrl.cancelCtx, "record leadership exiting...", nil)
			return
		}
	}
}

func (ctrl *controller) membershipChangedProcessor(ctx context.Context, event member.MembershipChangedEvent) error {
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()

	switch event.Type {
	case member.EventBecomeLeader:
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

		if err := ctrl.eventlogMgr.Run(ctx, ctrl.kvStore, true); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.ssMgr.Run(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}
	case member.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = false
		ctrl.eventlogMgr.Stop()
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
		ctrl.eventbusMap[filepath.Base(pair.Key)] = busInfo
	}
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
