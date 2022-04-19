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
	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/eventlog"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"path/filepath"
	"strings"
	"sync"
)

const (
	eventbusKeyPrefixInKVStore = "/vanus/internal/resource/eventbus"
)

func NewEventBusController(cfg Config, member embedetcd.Member) *controller {
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
	c.eventLogMgr = eventlog.NewManager(c.volumeMgr)
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
}

func (ctrl *controller) Start(ctx context.Context) error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvStore = store

	ctrl.cancelCtx, ctrl.cancelFunc = context.WithCancel(context.Background())
	go ctrl.member.RegisterMembershipChangedProcessor(ctrl.cancelCtx, ctrl.membershipChangedProcessor)
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

func (ctrl *controller) CreateEventBus(ctx context.Context, req *ctrlpb.CreateEventBusRequest) (*metapb.EventBus, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if req.LogNumber == 0 {
		req.LogNumber = 1
	}
	elNum := 1 // force set to 1 temporary
	eb := &metadata.Eventbus{
		ID:        vanus.NewID(), // TODO use another id generator
		Name:      req.Name,
		LogNumber: elNum,
		EventLogs: make([]*metadata.Eventlog, elNum),
	}
	exist, err := ctrl.kvStore.Exists(ctx, eb.Name)
	if err != nil {
		return nil, err
	}
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("already exist")
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
		if err := ctrl.kvStore.Set(ctx, ctrl.getEventBusKeyInKVStore(eb.Name), data); err != nil {
			return nil, err
		}
	}
	return &metapb.EventBus{
		Name:      eb.Name,
		LogNumber: int32(eb.LogNumber),
		Logs:      eventlog.Convert2ProtoEventLog(eb.EventLogs...),
		Id:        eb.ID.Uint64(),
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
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
	}

	ebMD := metadata.Convert2ProtoEventBus(_eb)[0]
	ebMD.Logs = eventlog.Convert2ProtoEventLog(_eb.EventLogs...)
	return ebMD, nil
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

	el := ctrl.eventLogMgr.GetEventLog(ctx, vanus.NewIDFromUint64(req.EventLogId))
	if el == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: eventlog.Convert2ProtoSegment(ctrl.eventLogMgr.GetEventLogSegmentList(el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

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
		volMD := &metadata.VolumeMetadata{ID: vanus.NewIDFromUint64(req.VolumeId)}
		_volInstance, err := ctrl.volumeMgr.RegisterVolume(ctx, volMD)
		if err != nil {
			return nil, err
		}
		volInstance = _volInstance
	}

	go func() {
		newCtx := context.Background()
		if err := srv.RemoteStart(newCtx); err == nil {
			ctrl.volumeMgr.UpdateRouting(newCtx, volInstance, srv)
		}
	}()
	return &ctrlpb.RegisterSegmentServerResponse{
		ServerId:      srv.ID().Uint64(),
		SegmentBlocks: volInstance.GetMeta().Blocks,
	}, nil
}

func (ctrl *controller) UnregisterSegmentServer(ctx context.Context,
	req *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

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
		t, err := util.ParseTime(req.ReportTime)
		if err != nil {
			log.Error(ctx, "parse heartbeat report time failed", map[string]interface{}{
				"volume_id":  req.VolumeId,
				"server_id":  req.ServerId,
				log.KeyError: err,
			})
			continue
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
		}
		srv.Polish()
	}

	if err != nil && err != io.EOF {
		log.Error(ctx, "block server heartbeat error", map[string]interface{}{
			log.KeyError: err,
		})
	}
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
	return &ctrlpb.GetAppendableSegmentResponse{Segments: eventlog.Convert2ProtoSegment(segInfos...)}, nil
}

func (ctrl *controller) ReportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	// TODO
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) getEventBusKeyInKVStore(ebName string) string {
	return strings.Join([]string{eventbusKeyPrefixInKVStore, ebName}, "/")
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

		if err := ctrl.eventLogMgr.Run(ctx, ctrl.kvStore); err != nil {
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
	pairs, err := ctrl.kvStore.List(ctx, eventbusKeyPrefixInKVStore)
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
	return nil
}

func (ctrl *controller) stop(ctx context.Context, err error) {
	ctrl.member.ResignIfLeader(ctx)
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
