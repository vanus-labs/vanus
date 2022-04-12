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
	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
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
	defaultAutoCreatedSegmentNumber = 3
	eventbusKeyPrefixInKVStore      = "/vanus/internal/resource/eventbus"
	eventlogKeyPrefixInKVStore      = "/vanus/internal/resource/eventlog"
	segmentBlockKeyPrefixInKVStore  = "/vanus/internal/resource/segmentBlock"
	volumeKeyPrefixInKVStore        = "/vanus/internal/resource/volume"
)

func NewEventBusController(cfg Config, member embedetcd.Member) *controller {
	c := &controller{
		cfg:         &cfg,
		eventLogMgr: nil,
		volumeMgr:   new(volumeMgr),
		eventBusMap: map[string]*info.BusInfo{},
		member:      member,
		isLeader:    false,
		readyNotify: make(chan struct{}, 1),
		stopNotify:  make(chan error, 1),
	}
	return c
}

type controller struct {
	cfg             *Config
	kvStore         kv.Client
	eventLogMgr     *eventlogManager
	volumeMgr       *volumeMgr
	ssMgr           *segmentServerManager
	eventBusMap     map[string]*info.BusInfo
	member          embedetcd.Member
	cancelCtx       context.Context
	cancelFunc      context.CancelFunc
	membershipMutex sync.Mutex
	isLeader        bool
	readyNotify     chan<- struct{}
	stopNotify      chan<- error
}

func (ctrl *controller) Start(ctx context.Context) error {
	store, err := etcd.NewEtcdClientV3(ctrl.cfg.KVStoreEndpoints, ctrl.cfg.KVKeyPrefix)
	if err != nil {
		return err
	}
	ctrl.cancelCtx, ctrl.cancelFunc = context.WithCancel(context.Background())
	ctrl.member.RegisterMembershipChangedProcessor(ctrl.cancelCtx, ctrl.membershipChangedProcessor)

	ctrl.kvStore = store

	if err = ctrl.volumeMgr.init(ctx, ctrl); err != nil {
		return err
	}

	ctrl.eventLogMgr = newEventlogManager(ctrl)
	if err = ctrl.eventLogMgr.start(ctx); err != nil {
		return err
	}

	if err := ctrl.eventLogMgr.initVolumeInfo(ctrl.volumeMgr); err != nil {
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
		return nil, err
	}
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("already exist")
	}
	for idx := 0; idx < eb.LogNumber; idx++ {
		el, err := ctrl.eventLogMgr.acquireEventLog(ctx)
		if err != nil {
			return nil, err
		}
		eb.EventLogs[idx] = el
		el.EventBusName = eb.Name
	}
	ctrl.eventBusMap[eb.Name] = eb

	// TODO add rollback handler when persist data to kv failed
	{
		data, _ := json.Marshal(eb)
		if err := ctrl.kvStore.Set(ctx, ctrl.getEventBusKeyInKVStore(eb.Name), data); err != nil {
			return nil, err
		}
		if err := ctrl.eventLogMgr.updateEventLog(ctx, eb.EventLogs...); err != nil {
			return nil, err
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
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
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
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}

	return &ctrlpb.ListSegmentResponse{
		Segments: info.Convert2ProtoSegment(ctrl.eventLogMgr.getEventLogSegmentList(ctx, el.ID)...),
	}, nil
}

func (ctrl *controller) RegisterSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	volumeInfo, err := ctrl.volumeMgr.registerVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}

	serverInfo, err := ctrl.ssMgr.AddServer(ctx, req.Address)
	if err != nil {
		return nil, err
	}

	ctrl.volumeMgr.refreshRoutingInfo(volumeInfo.ID(), serverInfo)

	go ctrl.ssMgr.remoteStartServer(serverInfo)
	return &ctrlpb.RegisterSegmentServerResponse{
		ServerId:      serverInfo.ID(),
		SegmentBlocks: volumeInfo.Blocks,
	}, nil
}

func (ctrl *controller) UnregisterSegmentServer(ctx context.Context,
	req *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	serverInfo := ctrl.ssMgr.GetServerInfoByAddress(req.Address)

	if serverInfo == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("segment server not found")
	}

	if err := ctrl.ssMgr.RemoveServer(ctx, serverInfo); err != nil {
		log.Warning(ctx, "remove server from segmentServerManager error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	ctrl.volumeMgr.refreshRoutingInfo(req.VolumeId, nil)
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

		serverInfo := ctrl.ssMgr.GetServerInfoByServerID(req.ServerId)
		if serverInfo == nil {
			log.Warning(ctx, "received a heartbeat request, but server info not found", map[string]interface{}{
				"volume_id": req.VolumeId,
				"server_id": req.ServerId,
			})
		}
		ctrl.ssMgr.polish(serverInfo)
	}

	if err != nil && err != io.EOF {
		log.Error(ctx, "segment server heartbeat error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return nil
}

func (ctrl *controller) GetAppendableSegment(ctx context.Context,
	req *ctrlpb.GetAppendableSegmentRequest) (*ctrlpb.GetAppendableSegmentResponse, error) {
	eli := ctrl.eventLogMgr.getEventLog(ctx, req.EventLogId)
	if eli == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	}
	num := int(req.Limited)
	if num == 0 {
		num = 1
	}
	segInfos, err := ctrl.eventLogMgr.getAppendableSegment(ctx, eli, num)
	if err != nil {
		return nil, err
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
		if err := ctrl.loadAllMetadata(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.volumeMgr.init(ctx, ctrl); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.eventLogMgr.start(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}

		if err := ctrl.ssMgr.start(ctx); err != nil {
			ctrl.stop(ctx, err)
			return err
		}
	case embedetcd.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = false
		ctrl.volumeMgr.destroy(ctx)
		ctrl.eventLogMgr.stop(ctx)
		ctrl.ssMgr.stop(ctx)
	}
	return nil
}

func (ctrl *controller) loadAllMetadata(ctx context.Context) error {
	// load eventbus info
	pairs, err := ctrl.kvStore.List(ctx, eventbusKeyPrefixInKVStore)
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
	return nil
}

func (ctrl *controller) stop(ctx context.Context, err error) {
	ctrl.member.ResignIfLeader(ctx)
	ctrl.cancelFunc()
	ctrl.stopNotify <- err
	ctrl.volumeMgr.destroy(ctx)
	ctrl.eventLogMgr.stop(ctx)
	ctrl.ssMgr.stop(ctx)
	if err := ctrl.kvStore.Close(); err != nil {
		log.Warning(ctx, "close kv client error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	close(ctrl.readyNotify)
	close(ctrl.stopNotify)
}
