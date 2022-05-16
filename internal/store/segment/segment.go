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

package segment

import (
	// standard libraries.
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	raftpb "github.com/linkall-labs/vsproto/pkg/raft"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	"github.com/linkall-labs/vanus/internal/util"
	errutil "github.com/linkall-labs/vanus/internal/util/errors"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	segmentServerDebugModeFlagENV = "SEGMENT_SERVER_DEBUG_MODE"
)

type segmentServer struct {
	blocks       sync.Map
	blockWriters sync.Map
	blockReaders sync.Map
	wal          *raftlog.WAL
	metaStore    *meta.SyncStore
	offsetStore  *meta.AsyncStore

	resolver *transport.SimpleResolver
	host     transport.Host

	id           vanus.ID
	state        primitive.ServerState
	isDebugMode  bool
	cfg          store.Config
	localAddress string

	volumeID  vanus.ID
	volumeDir string

	ctrlAddress []string
	credentials credentials.TransportCredentials
	cc          *ctrlClient

	stopCallback func()
	closeCh      chan struct{}
}

// Make sure segmentServer implements segpb.SegmentServerServer and primitive.Initializer.
var _ segpb.SegmentServerServer = (*segmentServer)(nil)
var _ primitive.Initializer = (*segmentServer)(nil)

func NewSegmentServer(cfg store.Config, stop func()) (segpb.SegmentServerServer, raftpb.RaftServerServer) {
	localAddress := fmt.Sprintf("%s:%d", cfg.IP, cfg.Port)

	// setup raft
	resolver := transport.NewSimpleResolver()
	host := transport.NewHost(resolver, localAddress)
	raftSrv := transport.NewRaftServer(context.TODO(), host)

	return &segmentServer{
		state:        primitive.ServerStateCreated,
		cfg:          cfg,
		localAddress: localAddress,
		volumeID:     cfg.Volume.ID,
		volumeDir:    cfg.Volume.Dir,
		resolver:     resolver,
		host:         host,
		ctrlAddress:  cfg.ControllerAddresses,
		credentials:  insecure.NewCredentials(),
		cc:           NewClient(cfg.ControllerAddresses),
		stopCallback: stop,
		closeCh:      make(chan struct{}),
	}, raftSrv
}

func (s *segmentServer) Initialize(ctx context.Context) error {
	if err := s.recover(ctx); err != nil {
		return err
	}

	if err := s.registerSelf(ctx); err != nil {
		return err
	}

	s.state = primitive.ServerStateStarted

	return nil
}

func (s *segmentServer) Start(ctx context.Context,
	req *segpb.StartSegmentServerRequest) (*segpb.StartSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if s.state != primitive.ServerStateStarted {
		return nil, errors.ErrServiceState.WithMessage("start failed, server state is not created")
	}
	if err := s.start(ctx); err != nil {
		return nil, errors.ErrInternal.WithMessage("start heartbeat task failed")
	}
	s.state = primitive.ServerStateRunning
	return &segpb.StartSegmentServerResponse{}, nil
}

func (s *segmentServer) Stop(ctx context.Context,
	req *segpb.StopSegmentServerRequest) (*segpb.StopSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't running, current state:%s", s.state))
	}

	s.state = primitive.ServerStateStopped
	if err := s.stop(ctx); err != nil {
		return nil, errors.ErrInternal.WithMessage("stop server failed")
	}

	s.stopCallback()
	return &segpb.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) Status(ctx context.Context,
	req *emptypb.Empty) (*segpb.StatusResponse, error) {
	return &segpb.StatusResponse{Status: string(s.state)}, nil
}

func (s *segmentServer) CreateBlock(ctx context.Context,
	req *segpb.CreateBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	if req.Id == 0 {
		log.Warning(ctx, "Can not create block without id.", nil)
		return nil, errors.ErrInvalidRequest.WithMessage("can not create block without id.")
	}

	log.Info(ctx, "Create block.", map[string]interface{}{
		"blockID": req.Id,
		"size":    req.Size,
	})

	blockID := vanus.NewIDFromUint64(req.Id)

	_, exist := s.blocks.Load(blockID)
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("the segment has already exist.")
	}

	// create block
	b, err := block.CreateFileSegmentBlock(ctx, filepath.Join(s.volumeDir, "block"), blockID, req.Size)
	if err != nil {
		return nil, err
	}

	// create replica
	replica := s.makeReplica(context.TODO(), b)

	s.blocks.Store(blockID, b)
	s.blockWriters.Store(blockID, replica)
	s.blockReaders.Store(blockID, b)

	return &emptypb.Empty{}, nil
}

func (s *segmentServer) RemoveBlock(ctx context.Context,
	req *segpb.RemoveBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	// TODO(james.yin): remove block

	return &emptypb.Empty{}, nil
}

// ActivateSegment mark a block ready to using and preparing to initializing a replica group.
func (s *segmentServer) ActivateSegment(ctx context.Context,
	req *segpb.ActivateSegmentRequest) (*segpb.ActivateSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	if len(req.Replicas) == 0 {
		log.Warning(ctx, "Replicas can not be empty.", map[string]interface{}{
			"eventLogID":     req.EventLogId,
			"replicaGroupID": req.ReplicaGroupId,
		})
		return &segpb.ActivateSegmentResponse{}, nil
	}

	log.Info(ctx, "Activate segment.", map[string]interface{}{
		"eventLogID":     req.EventLogId,
		"replicaGroupID": req.ReplicaGroupId,
		"replicas":       req.Replicas,
	})

	var myID vanus.ID
	peers := make([]block.IDAndEndpoint, 0, len(req.Replicas)-1)
	for blockID, endpoint := range req.Replicas {
		peer := vanus.NewIDFromUint64(blockID)
		peers = append(peers, block.IDAndEndpoint{
			ID:       peer,
			Endpoint: endpoint,
		})
		if endpoint == s.localAddress {
			myID = peer
		}
	}

	if myID == 0 {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
	}
	v, exist := s.blockWriters.Load(myID)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
	}

	// Register peers.
	for i := range peers {
		peer := &peers[i]
		s.resolver.Register(peer.ID.Uint64(), peer.Endpoint)
	}

	log.Info(ctx, "Bootstrap replica.", map[string]interface{}{
		"blockID": myID,
		"peers":   peers,
	})

	// Bootstrap replica.
	replica, _ := v.(*block.Replica)
	if err := replica.Bootstrap(peers); err != nil {
		return nil, err
	}

	return &segpb.ActivateSegmentResponse{}, nil
}

// InactivateSegment mark a block ready to be removed.
func (s *segmentServer) InactivateSegment(ctx context.Context,
	req *segpb.InactivateSegmentRequest) (*segpb.InactivateSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	return &segpb.InactivateSegmentResponse{}, nil
}

func (s *segmentServer) GetBlockInfo(ctx context.Context,
	req *segpb.GetBlockInfoRequest) (*segpb.GetBlockInfoResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	return &segpb.GetBlockInfoResponse{}, nil
}

// AppendToBlock implements segpb.SegmentServerServer.
func (s *segmentServer) AppendToBlock(ctx context.Context,
	req *segpb.AppendToBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkState(); err != nil {
		return nil, err
	}

	if req.Events == nil || len(req.Events.Events) == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("event list is empty")
	}

	blockID := vanus.NewIDFromUint64(req.BlockId)

	v, exist := s.blockWriters.Load(blockID)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the block doesn't exist")
	}

	writer, _ := v.(block.SegmentBlockWriter)
	if !writer.IsAppendable() {
		return nil, errors.ErrSegmentNoEnoughCapacity
	}

	events := req.GetEvents().Events
	entries := make([]block.Entry, len(events))
	for i, event := range events {
		payload, err := proto.Marshal(event)
		if err != nil {
			return nil, errors.ErrInternal.WithMessage("marshall event failed").Wrap(err)
		}
		entries[i] = block.Entry{
			Payload: payload,
		}
	}

	if err := writer.Append(ctx, entries...); err != nil {
		if err == block.ErrNoEnoughCapacity {
			// TODO optimize this to async from sync
			if err = s.markSegmentIsFull(ctx, blockID); err != nil {
				return nil, err
			}
			return nil, errors.ErrSegmentNoEnoughCapacity
		}
		return nil, errors.ErrInternal.WithMessage("write to storage failed").Wrap(err)
	}
	return &emptypb.Empty{}, nil
}

// ReadFromBlock implements segpb.SegmentServerServer.
func (s *segmentServer) ReadFromBlock(ctx context.Context,
	req *segpb.ReadFromBlockRequest) (*segpb.ReadFromBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	if err := s.checkState(); err != nil {
		return nil, err
	}

	blockID := vanus.NewIDFromUint64(req.BlockId)
	segV, exist := s.blocks.Load(blockID)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist on this server")
	}

	segBlock, _ := segV.(block.SegmentBlock)
	v, exist := s.blockReaders.Load(blockID)
	var reader block.SegmentBlockReader
	if !exist {
		_reader, err := block.OpenFileSegmentBlock(ctx, segBlock.Path())
		if err != nil {
			return nil, err
		}
		reader = _reader
		s.blockReaders.Store(blockID, reader)
	} else {
		reader, _ = v.(block.SegmentBlockReader)
	}
	entries, err := reader.Read(ctx, int(req.Offset), int(req.Number))
	if err != nil {
		return nil, err
	}

	events := make([]*cepb.CloudEvent, len(entries))
	for i, entry := range entries {
		event := &cepb.CloudEvent{}
		if err := proto.Unmarshal(entry.Payload, event); err != nil {
			return nil, errors.ErrInternal.WithMessage("unmarshall data to event failed").Wrap(err)
		}
		events[i] = event
	}

	return &segpb.ReadFromBlockResponse{
		Events: &cepb.CloudEventBatch{Events: events},
	}, nil
}

func (s *segmentServer) startHeartbeatTask(ctx context.Context) error {
	if s.isDebugMode {
		return nil
	}

	go s.runHeartbeat(ctx)
	return nil
}

func (s *segmentServer) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			infos := make([]*metapb.SegmentHealthInfo, 0)
			s.blocks.Range(func(key, value interface{}) bool {
				b, _ := value.(block.SegmentBlock)
				infos = append(infos, b.HealthInfo())
				return true
			})
			req := &ctrlpb.SegmentHeartbeatRequest{
				ServerId:   s.id.Uint64(),
				VolumeId:   s.volumeID.Uint64(),
				HealthInfo: infos,
				ReportTime: util.FormatTime(time.Now()),
				ServerAddr: s.localAddress,
			}
			if err := s.cc.heartbeat(context.Background(), req); err != nil {
				log.Warning(
					ctx,
					"Send heartbeat to controller failed, connection lost. try to reconnecting",
					map[string]interface{}{
						log.KeyError: err,
					})
			}
		}
	}
}

func (s *segmentServer) start(ctx context.Context) error {
	log.Info(ctx, "Start SegmentServer.", nil)
	if err := s.startHeartbeatTask(ctx); err != nil {
		return err
	}
	return nil
}

func (s *segmentServer) stop(ctx context.Context) error {
	s.cc.Close(ctx)
	wg := sync.WaitGroup{}
	var err error
	{
		wg.Add(1)
		go func() {
			s.waitAllAppendRequestCompleted(ctx)
			s.blockWriters.Range(func(key, value interface{}) bool {
				writer, _ := value.(block.SegmentBlockWriter)
				if err2 := writer.CloseWrite(ctx); err2 != nil {
					err = errutil.Chain(err, err2)
				}
				return true
			})
			wg.Done()
		}()
	}

	{
		wg.Add(1)
		go func() {
			s.waitAllReadRequestCompleted(ctx)
			s.blockReaders.Range(func(key, value interface{}) bool {
				reader, _ := value.(block.SegmentBlockReader)
				if err2 := reader.CloseRead(ctx); err2 != nil {
					err = errutil.Chain(err, err2)
				}
				return true
			})
			wg.Done()
		}()
	}

	wg.Wait()
	return err
}

func (s *segmentServer) markSegmentIsFull(ctx context.Context, segId vanus.ID) error {
	bl, exist := s.blocks.Load(segId)
	if !exist {
		return fmt.Errorf("the SegmentBlock does not exist")
	}

	writer, _ := bl.(block.SegmentBlockWriter)
	if err := writer.CloseWrite(ctx); err != nil {
		return err
	}

	// report to controller immediately
	_, err := s.cc.reportSegmentBlockIsFull(ctx, &ctrlpb.SegmentHeartbeatRequest{
		ServerId: s.id.Uint64(),
		VolumeId: s.volumeID.Uint64(),
		HealthInfo: []*metapb.SegmentHealthInfo{
			bl.(block.SegmentBlock).HealthInfo(),
		},
		ReportTime: util.FormatTime(time.Now()),
	})
	return err
}

func (s *segmentServer) waitAllAppendRequestCompleted(ctx context.Context) {}

func (s *segmentServer) waitAllReadRequestCompleted(ctx context.Context) {}

func (s *segmentServer) registerSelf(ctx context.Context) error {
	if strings.ToLower(os.Getenv(segmentServerDebugModeFlagENV)) == "true" {
		return s.registerSelfInDebug(ctx)
	}

	res, err := s.cc.registerSegmentServer(ctx, &ctrlpb.RegisterSegmentServerRequest{
		Address:  s.localAddress,
		VolumeId: s.volumeID.Uint64(),
		Capacity: s.cfg.Volume.Capacity,
	})
	if err != nil {
		return err
	}

	s.id = vanus.NewIDFromUint64(res.ServerId)

	// FIXME(james.yin): some blocks may not be bound to segment.

	// No block in the volume of this server.
	if len(res.Segments) == 0 {
		return nil
	}

	for _, segmentpb := range res.Segments {
		if len(segmentpb.Replicas) == 0 {
			continue
		}
		var myID vanus.ID
		for blockID, blockpb := range segmentpb.Replicas {
			// Don't use address to compare
			if blockpb.VolumeID == s.volumeID.Uint64() {
				if myID != 0 {
					// FIXME(james.yin): multiple blocks of same segment in this server.
				}
				myID = vanus.NewIDFromUint64(blockID)
			}
		}
		if myID == 0 {
			// TODO(james.yin): no my block
			continue
		}
		// Register peers.
		for blockID, blockpb := range segmentpb.Replicas {
			if blockpb.Endpoint == "" {
				if blockpb.VolumeID == s.volumeID.Uint64() {
					blockpb.Endpoint = s.localAddress
				} else {
					log.Warning(ctx, "Block is offline.", map[string]interface{}{
						"blockID":    blockID,
						"volumeID":   blockpb.VolumeID,
						"segmentID":  segmentpb.Id,
						"eventlogID": segmentpb.EventLogId,
					})
					continue
				}
			}
			s.resolver.Register(blockID, blockpb.Endpoint)
		}
	}

	return nil
}

func (s *segmentServer) registerSelfInDebug(ctx context.Context) error {
	log.Info(ctx, "the segment server debug mode enabled", nil)

	s.id = vanus.NewID()
	s.isDebugMode = true

	return nil
}

func (s *segmentServer) makeReplica(ctx context.Context, b block.SegmentBlock) *block.Replica {
	raftLog := raftlog.NewLog(b.SegmentBlockID(), s.wal, s.metaStore, s.offsetStore)
	return s.makeReplicaWithRaftLog(ctx, b, raftLog)
}

func (s *segmentServer) makeReplicaWithRaftLog(ctx context.Context, b block.SegmentBlock, raftLog *raftlog.Log) *block.Replica {
	replica := block.NewReplica(ctx, b, raftLog, s.host)
	s.host.Register(b.SegmentBlockID().Uint64(), replica)
	return replica
}

func (s *segmentServer) checkState() error {
	if s.state != primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't ready to work, current state:%s", s.state))
	}
	return nil
}
