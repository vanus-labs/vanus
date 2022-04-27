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
	// standard libraries
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	raftpb "github.com/linkall-labs/vsproto/pkg/raft"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"

	// this project
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
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
	wal          *walog.WAL

	resolver *transport.SimpleResolver
	host     transport.Host

	id           vanus.ID
	state        primitive.ServerState
	isDebugMode  bool
	cfg          store.Config
	localAddress string

	volumeId  vanus.ID
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
		volumeId:     cfg.Volume.ID,
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
	// recover wal and raft log
	raftLogs, wal, err := raftlog.RecoverLogsAndWAL(filepath.Join(s.volumeDir, "wal"))
	if err != nil {
		return err
	}
	s.wal = wal

	blockPeers, err := s.registerSelf(ctx)
	if err != nil {
		return err
	}

	if err = s.recoverBlocks(ctx, blockPeers, raftLogs); err != nil {
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

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	blockID := vanus.NewIDFromUint64(req.Id)

	_, exist := s.blocks.Load(blockID)
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("the segment has already exist")
	}

	// create block
	path := s.generateNewBlockPath(blockID)
	b, err := block.CreateFileSegmentBlock(ctx, blockID, path, req.Size)
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

	if err := s.checkoutState(); err != nil {
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

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	// bootstrap replica
	if len(req.Replicas) > 0 {
		var myID vanus.ID
		var peers []block.IDAndEndpoint
		for blockID, endpoint := range req.Replicas {
			peer := vanus.NewIDFromUint64(blockID)
			peers = append(peers, block.IDAndEndpoint{
				ID:       peer,
				Endpoint: endpoint,
			})
			if endpoint == s.localAddress {
				myID = peer
			} else {
				// register peer
				s.resolver.Register(blockID, endpoint)
			}
		}

		if myID == 0 {
			return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
		}
		v, exist := s.blockWriters.Load(myID)
		if !exist {
			return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
		}

		replica := v.(*block.Replica)
		if err := replica.Bootstrap(peers); err != nil {
			return nil, err
		}
	}

	return &segpb.ActivateSegmentResponse{}, nil
}

// InactivateSegment mark a block ready to be removed.
func (s *segmentServer) InactivateSegment(ctx context.Context,
	req *segpb.InactivateSegmentRequest) (*segpb.InactivateSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &segpb.InactivateSegmentResponse{}, nil
}

func (s *segmentServer) GetBlockInfo(ctx context.Context,
	req *segpb.GetBlockInfoRequest) (*segpb.GetBlockInfoResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &segpb.GetBlockInfoResponse{}, nil
}

// AppendToBlock implements segpb.SegmentServerServer.
func (s *segmentServer) AppendToBlock(ctx context.Context,
	req *segpb.AppendToBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
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
	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	blockID := vanus.NewIDFromUint64(req.BlockId)
	segV, exist := s.blocks.Load(blockID)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist on this server")
	}

	segBlock := segV.(block.SegmentBlock)
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
		reader = v.(block.SegmentBlockReader)
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

func (s *segmentServer) startHeartbeatTask() error {
	if s.isDebugMode {
		return nil
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		ctx := context.Background()

	LOOP:
		for {
			select {
			case <-s.closeCh:
				break LOOP
			case <-ticker.C:
				infos := make([]*metapb.SegmentHealthInfo, 0)
				s.blocks.Range(func(key, value interface{}) bool {
					infos = append(infos, value.(block.SegmentBlock).HealthInfo())
					return true
				})
				req := &ctrlpb.SegmentHeartbeatRequest{
					ServerId:   s.id.Uint64(),
					VolumeId:   s.volumeId.Uint64(),
					HealthInfo: infos,
					ReportTime: util.FormatTime(time.Now()),
					ServerAddr: s.localAddress,
				}
				if err := s.cc.heartbeat(context.Background(), req); err != nil {
					log.Warning(ctx, "send heartbeat to controller failed, connection lost. try to reconnecting", map[string]interface{}{
						log.KeyError: err,
					})
				}
			}
		}
		ticker.Stop()
	}()
	return nil
}

func (s *segmentServer) generateNewBlockPath(id vanus.ID) string {
	return filepath.Join(s.volumeDir, "block", id.String())
}

func (s *segmentServer) start(ctx context.Context) error {
	wg := sync.WaitGroup{}

	var err error
	s.blocks.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(segBlock block.SegmentBlock) {
			if _err := segBlock.Initialize(ctx); err != nil {
				err = errutil.Chain(err, _err)
			}
			// s.blockWriters.Store(segBlock.SegmentBlockID(), segBlock)
			s.blockReaders.Store(segBlock.SegmentBlockID(), segBlock)
			wg.Done()
		}(value.(block.SegmentBlock))
		return true
	})
	wg.Wait()

	if err := s.startHeartbeatTask(); err != nil {
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
				writer := value.(block.SegmentBlockWriter)
				if _err := writer.CloseWrite(ctx); err != nil {
					err = errutil.Chain(err, _err)
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
				reader := value.(block.SegmentBlockReader)
				if _err := reader.CloseRead(ctx); err != nil {
					err = errutil.Chain(err, _err)
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

	if err := bl.(block.SegmentBlockWriter).CloseWrite(ctx); err != nil {
		return err
	}

	// report to controller immediately
	_, err := s.cc.reportSegmentBlockIsFull(ctx, &ctrlpb.SegmentHeartbeatRequest{
		ServerId: s.id.Uint64(),
		VolumeId: s.volumeId.Uint64(),
		HealthInfo: []*metapb.SegmentHealthInfo{
			bl.(block.SegmentBlock).HealthInfo(),
		},
		ReportTime: util.FormatTime(time.Now()),
	})
	return err
}

func (s *segmentServer) waitAllAppendRequestCompleted(ctx context.Context) {}

func (s *segmentServer) waitAllReadRequestCompleted(ctx context.Context) {}

func (s *segmentServer) registerSelf(ctx context.Context) (map[vanus.ID][]uint64, error) {
	if strings.ToLower(os.Getenv(segmentServerDebugModeFlagENV)) == "true" {
		return s.registerSelfInDebug(ctx)
	}

	res, err := s.cc.registerSegmentServer(ctx, &ctrlpb.RegisterSegmentServerRequest{
		Address:  s.localAddress,
		VolumeId: s.volumeId.Uint64(),
		Capacity: s.cfg.Volume.Capacity,
	})
	if err != nil {
		return nil, err
	}

	s.id = vanus.NewIDFromUint64(res.ServerId)

	// FIXME(james.yin): some blocks may not be bound to segment.

	// No block in the volume of this server.
	if len(res.Segments) <= 0 {
		return map[vanus.ID][]uint64{}, nil
	}

	blockPeers := make(map[vanus.ID][]uint64)
	for _, segmentpb := range res.Segments {
		if len(segmentpb.Replicas) <= 0 {
			continue
		}
		var myID vanus.ID
		peers := make([]uint64, 0, len(segmentpb.Replicas))
		for blockID, blockpb := range segmentpb.Replicas {
			peers = append(peers, blockID)
			// Don't use address to compare
			if blockpb.VolumeID == s.volumeId.Uint64() {
				if myID != 0 {
					// FIXME(james.yin): multiple blocks of same segment in this server.
				}
				myID = vanus.NewIDFromUint64(blockID)
			} else {
				// register peer
				s.resolver.Register(blockID, blockpb.Endpoint)
			}
		}
		if myID == 0 {
			// TODO(james.yin): no my block
			continue
		}
		blockPeers[myID] = peers
	}
	return blockPeers, nil
}

func (s *segmentServer) registerSelfInDebug(ctx context.Context) (map[vanus.ID][]uint64, error) {
	log.Info(ctx, "the segment server debug mode enabled", nil)

	s.id = vanus.NewID()
	s.isDebugMode = true

	files, err := filepath.Glob(filepath.Join(s.volumeDir, "block", "*"))
	if err != nil {
		return nil, err
	}

	blockPeers := make(map[vanus.ID][]uint64)
	for _, file := range files {
		blockID, err := strconv.ParseUint(filepath.Base(file), 10, 64)
		if err != nil {
			return nil, err
		}
		// TODO(james.yin): multiple peers
		blockPeers[vanus.NewIDFromUint64(blockID)] = []uint64{blockID}
	}
	return blockPeers, nil
}

func (s *segmentServer) recoverBlocks(ctx context.Context, blockPeers map[vanus.ID][]uint64, raftLogs map[vanus.ID]*raftlog.Log) error {
	blockDir := filepath.Join(s.volumeDir, "block")

	// Make sure the block directory exists.
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		return err
	}

	// TODO: optimize this, because the implementation assumes under storage is linux file system
	for blockID, peers := range blockPeers {
		blockPath := filepath.Join(blockDir, blockID.String())
		b, err := block.OpenFileSegmentBlock(ctx, blockPath)
		if err != nil {
			return err
		}
		log.Info(ctx, "the block was loaded", map[string]interface{}{
			"id": b.SegmentBlockID().String(),
		})
		// TODO(james.yin): initialize block

		s.blocks.Store(blockID, b)

		// recover replica
		if b.IsAppendable() {
			raftLog := raftLogs[blockID]
			// raft log has been compacted
			if raftLog == nil {
				raftLog = raftlog.NewLog(blockID, s.wal, peers)
			} else {
				// TODO(james.yin): set peers
			}
			replica := s.makeReplicaWithRaftLog(context.TODO(), b, raftLog)
			s.blockWriters.Store(b.SegmentBlockID(), replica)
		}
	}

	for blockID, raftLog := range raftLogs {
		b, ok := s.blocks.Load(blockID)
		if !ok {
			// TODO(james.yin): no block for raft log, compact
		}
		if _, ok = b.(*block.Replica); !ok {
			// TODO(james.yin): block is not appendable, compact
		}
		_ = raftLog
	}

	return nil
}

func (s *segmentServer) makeReplica(ctx context.Context, b block.SegmentBlock) *block.Replica {
	raftLog := raftlog.NewLog(b.SegmentBlockID(), s.wal, nil)
	return s.makeReplicaWithRaftLog(ctx, b, raftLog)
}

func (s *segmentServer) makeReplicaWithRaftLog(ctx context.Context, b block.SegmentBlock, raftLog *raftlog.Log) *block.Replica {
	replica := block.NewReplica(ctx, b, raftLog, s.host)
	s.host.Register(b.SegmentBlockID().Uint64(), replica)
	return replica
}

func (s *segmentServer) checkoutState() error {
	if s.state != primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't ready to work, current state:%s", s.state))
	}
	return nil
}
