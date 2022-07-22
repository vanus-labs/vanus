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

//go:generate mockgen -source=server.go -destination=mock_server.go -package=segment
package segment

import (
	// standard libraries.
	"context"
	stderr "errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/proto"

	// first-party libraries.
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	rpcerr "github.com/linkall-labs/vanus/proto/pkg/errors"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	raftpb "github.com/linkall-labs/vanus/proto/pkg/raft"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/block/file"
	"github.com/linkall-labs/vanus/internal/store/block/replica"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	debugModeENV                = "SEGMENT_SERVER_DEBUG_MODE"
	defaultLeaderInfoBufferSize = 256
	defaultForceStopTimeout     = 30 * time.Second
)

type Server interface {
	primitive.Initializer

	Serve(lis net.Listener) error

	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status() primitive.ServerState

	CreateBlock(ctx context.Context, id vanus.ID, size int64) error
	RemoveBlock(ctx context.Context, id vanus.ID) error
	// GetBlockInfo(ctx context.Context, id vanus.ID) error

	ActivateSegment(ctx context.Context, logID vanus.ID, segID vanus.ID, replicas map[vanus.ID]string) error
	InactivateSegment(ctx context.Context) error

	AppendToBlock(ctx context.Context, id vanus.ID, events []*cepb.CloudEvent) error
	ReadFromBlock(ctx context.Context, id vanus.ID, off int, num int) ([]*cepb.CloudEvent, error)
}

func NewServer(cfg store.Config) Server {
	var debugModel bool
	if strings.ToLower(os.Getenv(debugModeENV)) == "true" {
		debugModel = true
	}

	localAddress := fmt.Sprintf("%s:%d", cfg.IP, cfg.Port)

	// Setup raft.
	resolver := transport.NewSimpleResolver()
	host := transport.NewHost(resolver, localAddress)

	srv := &server{
		state:        primitive.ServerStateCreated,
		cfg:          cfg,
		isDebugMode:  debugModel,
		localAddress: localAddress,
		volumeID:     cfg.Volume.ID,
		volumeDir:    cfg.Volume.Dir,
		resolver:     resolver,
		host:         host,
		ctrlAddress:  cfg.ControllerAddresses,
		credentials:  insecure.NewCredentials(),
		cc:           NewClient(cfg.ControllerAddresses),
		leaderc:      make(chan leaderInfo, defaultLeaderInfoBufferSize),
		closec:       make(chan struct{}),
	}

	return srv
}

type leaderInfo struct {
	leader vanus.ID
	term   uint64
}

type server struct {
	blocks  sync.Map // block.ID, *file.Block
	writers sync.Map // block.ID, replica.Replica
	// TODO(weihe.yin) delete this
	readers sync.Map // block.ID, *file.Block

	wal         *raftlog.WAL
	metaStore   *meta.SyncStore
	offsetStore *meta.AsyncStore

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
	leaderc     chan leaderInfo

	grpcSrv *grpc.Server
	closec  chan struct{}
}

// Make sure server implements Server.
var _ Server = (*server)(nil)

func (s *server) Serve(lis net.Listener) error {
	segSrv := &segmentServer{
		srv: s,
	}

	raftSrv := transport.NewServer(s.host)

	srv := grpc.NewServer(grpc.InTapHandle(s.preGrpcStream))
	segpb.RegisterSegmentServerServer(srv, segSrv)
	raftpb.RegisterRaftServerServer(srv, raftSrv)
	s.grpcSrv = srv

	return srv.Serve(lis)
}

func (s *server) preGrpcStream(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName == "/linkall.vanus.raft.RaftServer/SendMessage" {
		cctx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-cctx.Done():
			case <-s.closec:
				cancel()
			}
		}()
		return cctx, nil
	}
	return ctx, nil
}

func (s *server) Initialize(ctx context.Context) error {
	// Recover state from volume.
	if err := s.recover(ctx); err != nil {
		return err
	}

	// Fetch block information in volume from controller, and make state up to date.
	if err := s.reconcileBlocks(ctx); err != nil {
		return err
	}

	s.state = primitive.ServerStateStarted

	if !s.isDebugMode {
		// Register to controller.
		if err := s.registerSelf(ctx); err != nil {
			return err
		}
	} else {
		log.Info(ctx, "the segment server debug mode enabled", nil)

		s.id = vanus.NewID()
		if err := s.Start(ctx); err != nil {
			return err
		}
		s.state = primitive.ServerStateRunning
	}

	return nil
}

func (s *server) reconcileBlocks(ctx context.Context) error {
	// TODO(james.yin): Fetch block information in volume from controller, and make state up to date.
	return nil
}

func (s *server) registerSelf(ctx context.Context) error {
	// TODO(james.yin): pass information of blocks.
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

	s.reconcileSegments(ctx, res.Segments)

	return nil
}

func (s *server) reconcileSegments(ctx context.Context, segmentpbs map[uint64]*metapb.Segment) {
	for _, segmentpb := range segmentpbs {
		if len(segmentpb.Replicas) == 0 {
			continue
		}
		var myID vanus.ID
		for blockID, blockpb := range segmentpb.Replicas {
			// Don't use address to compare
			if blockpb.VolumeID == s.volumeID.Uint64() {
				if myID != 0 {
					// FIXME(james.yin): multiple blocks of same segment in this server.
					log.Warning(ctx, "Multiple blocks of the same segment in this server.", map[string]interface{}{
						"blockID":   blockID,
						"other":     myID,
						"segmentID": segmentpb.Id,
						"volumeID":  s.volumeID,
					})
				}
				myID = vanus.NewIDFromUint64(blockID)
			}
		}
		if myID == 0 {
			// TODO(james.yin): no my block
			log.Warning(ctx, "No block of the specific segment in this server.", map[string]interface{}{
				"segmentID": segmentpb.Id,
				"volumeID":  s.volumeID,
			})
			continue
		}
		s.registerReplicas(ctx, segmentpb)
	}
}

func (s *server) registerReplicas(ctx context.Context, segmentpb *metapb.Segment) {
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

func (s *server) Start(ctx context.Context) error {
	if s.state != primitive.ServerStateStarted {
		return errors.ErrServiceState.WithMessage(
			"start failed, server state is not created")
	}

	log.Info(ctx, "Start SegmentServer.", nil)
	if err := s.startHeartbeatTask(ctx); err != nil {
		return errors.ErrInternal.WithMessage("start heartbeat task failed")
	}

	s.state = primitive.ServerStateRunning
	return nil
}

func (s *server) startHeartbeatTask(ctx context.Context) error {
	if s.isDebugMode {
		return nil
	}

	go s.runHeartbeat(ctx)
	return nil
}

func (s *server) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closec:
			return
		case <-ticker.C:
			infos := make([]*metapb.SegmentHealthInfo, 0)
			s.blocks.Range(func(key, value interface{}) bool {
				b, _ := value.(block.Block)
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
		case info := <-s.leaderc:
			// TODO(james.yin): move to other goroutine.
			req := &ctrlpb.ReportSegmentLeaderRequest{
				LeaderId: info.leader.Uint64(),
				Term:     info.term,
			}
			if err := s.cc.reportSegmentLeader(context.Background(), req); err != nil {
				log.Debug(ctx, "Report segment leader to controller failed.", map[string]interface{}{
					"leader":     info.leader,
					"term":       info.term,
					log.KeyError: err,
				})
			}
		}
	}
}

func (s *server) leaderChanged(blockID, leaderID vanus.ID, term uint64) {
	if blockID == leaderID {
		info := leaderInfo{
			leader: leaderID,
			term:   term,
		}

		select {
		case s.leaderc <- info:
		default:
		}
	}
}

func (s *server) Stop(ctx context.Context) error {
	if s.state != primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't running, current state:%s", s.state))
	}

	s.state = primitive.ServerStateStopped

	// TODO(james.yin): async
	if err := s.stop(ctx); err != nil {
		return errors.ErrInternal.WithMessage("stop server failed")
	}

	// Stop grpc asynchronously.
	go func() {
		// Force stop if timeout.
		t := time.AfterFunc(defaultForceStopTimeout, func() {
			log.Warning(context.Background(), "Graceful stop timeout, force stop.", nil)
			s.grpcSrv.Stop()
		})
		defer t.Stop()
		s.grpcSrv.GracefulStop()
	}()

	return nil
}

func (s *server) stop(ctx context.Context) error {
	// Close all raft nodes.
	s.writers.Range(func(key, value interface{}) bool {
		r, _ := value.(replica.Replica)
		r.Stop(ctx)
		return true
	})

	// Close all blocks.
	s.blocks.Range(func(key, value interface{}) bool {
		b, _ := value.(*file.Block)
		_ = b.Close(context.TODO())
		return true
	})

	// Close WAL, metaStore, offsetStore.
	s.wal.Close()
	s.offsetStore.Close()
	// Make sure WAL is closed before close metaStore.
	s.wal.Wait()
	s.metaStore.Close()

	// Stop heartbeat task, etc.
	close(s.closec)

	// Close grpc connections for raft.
	s.host.Stop()

	s.cc.Close(ctx)

	return nil
}

func (s *server) Status() primitive.ServerState {
	return s.state
}

func (s *server) CreateBlock(ctx context.Context, id vanus.ID, size int64) error {
	if id == 0 {
		log.Warning(ctx, "Can not create block with id(0).", nil)
		return errors.ErrInvalidRequest.WithMessage("can not create block with id(0).")
	}

	if err := s.checkState(); err != nil {
		return err
	}

	log.Info(ctx, "Create block.", map[string]interface{}{
		"blockID": id,
		"size":    size,
	})

	if _, ok := s.blocks.Load(id); ok {
		return errors.ErrResourceAlreadyExist.WithMessage("the segment has already exist.")
	}

	// Create block.
	b, err := file.Create(ctx, filepath.Join(s.volumeDir, "block"), id, size)
	if err != nil {
		return err
	}

	// Create replica.
	r := s.makeReplica(context.TODO(), b.ID(), b, b)
	b.SetClusterInfoSource(r)

	s.blocks.Store(id, b)
	s.writers.Store(id, r)
	s.readers.Store(id, b)

	return nil
}

func (s *server) makeReplica(
	ctx context.Context, blockID vanus.ID, appender block.TwoPCAppender, snapOp raftlog.SnapshotOperator,
) replica.Replica {
	raftLog := raftlog.NewLog(blockID, s.wal, s.metaStore, s.offsetStore, snapOp)
	return s.makeReplicaWithRaftLog(ctx, blockID, appender, raftLog)
}

func (s *server) makeReplicaWithRaftLog(
	ctx context.Context, blockID vanus.ID, appender block.TwoPCAppender, raftLog *raftlog.Log,
) replica.Replica {
	r := replica.New(ctx, blockID, appender, raftLog, s.host, s.leaderChanged)
	s.host.Register(blockID.Uint64(), r)
	return r
}

func (s *server) RemoveBlock(ctx context.Context, blockID vanus.ID) error {
	if err := s.checkState(); err != nil {
		return err
	}

	err := s.removeReplica(ctx, blockID)
	if err != nil {
		return err
	}
	s.readers.Delete(blockID)
	v, exist := s.blocks.LoadAndDelete(blockID)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("the block not found")
	}
	blk, _ := v.(*file.Block)
	// TODO(weihe.yin) s.host.Unregister
	if err = blk.Destroy(ctx); err != nil {
		return err
	}
	log.Info(ctx, "the block has been deleted", map[string]interface{}{
		"id":       blk.ID(),
		"path":     blk.Path(),
		"metadata": blk.HealthInfo().String(),
	})
	return nil
}

func (s *server) removeReplica(ctx context.Context, blockID vanus.ID) error {
	if err := s.checkState(); err != nil {
		return err
	}

	v, exist := s.writers.Load(blockID)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("the replica not found")
	}
	rp, _ := v.(replica.Replica)
	rp.Delete(ctx)

	s.writers.Delete(blockID)
	return nil
}

// TODO(james.yin):
// func (s *server) GetBlockInfo(ctx context.Context, id vanus.ID) error {
// 	if err := s.checkState(); err != nil {
// 		return err
// 	}
// 	return nil
// }

// ActivateSegment mark a block ready to using and preparing to initializing a replica group.
func (s *server) ActivateSegment(
	ctx context.Context, logID vanus.ID, segID vanus.ID, replicas map[vanus.ID]string,
) error {
	if err := s.checkState(); err != nil {
		return err
	}

	if len(replicas) == 0 {
		log.Warning(ctx, "Replicas can not be empty.", map[string]interface{}{
			"eventLogID":     logID,
			"replicaGroupID": segID,
		})
		return nil
	}

	log.Info(ctx, "Activate segment.", map[string]interface{}{
		"eventLogID":     logID,
		"replicaGroupID": segID,
		"replicas":       replicas,
	})

	var myID vanus.ID
	peers := make([]replica.Peer, 0, len(replicas))
	for blockID, endpoint := range replicas {
		peer := replica.Peer{
			ID:       blockID,
			Endpoint: endpoint,
		}
		peers = append(peers, peer)
		if endpoint == s.localAddress {
			myID = blockID
		}
	}

	if myID == 0 {
		return errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
	}
	v, ok := s.writers.Load(myID)
	if !ok {
		return errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
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
	replica, _ := v.(replica.Replica)
	if err := replica.Bootstrap(peers); err != nil {
		return err
	}

	return nil
}

// InactivateSegment mark a block ready to be removed. This method is usually used for data transfer.
func (s *server) InactivateSegment(ctx context.Context) error {
	if err := s.checkState(); err != nil {
		return err
	}
	return nil
}

func (s *server) AppendToBlock(ctx context.Context, id vanus.ID, events []*cepb.CloudEvent) error {
	if len(events) == 0 {
		return errors.ErrInvalidRequest.WithMessage("event list is empty")
	}

	if err := s.checkState(); err != nil {
		return err
	}

	var appender block.Appender
	if v, ok := s.writers.Load(id); ok {
		appender, _ = v.(block.Appender)
	} else {
		return errors.ErrResourceNotFound.WithMessage("the block doesn't exist")
	}

	entries := make([]block.Entry, len(events))
	for i, event := range events {
		payload, err := proto.Marshal(event)
		if err != nil {
			return errors.ErrInternal.WithMessage("marshall event failed").Wrap(err)
		}
		entries[i] = block.Entry{
			Payload: payload,
		}
	}

	if err := appender.Append(ctx, entries...); err != nil {
		return s.processAppendError(ctx, id, err)
	}

	return nil
}

func (s *server) processAppendError(ctx context.Context, blockID vanus.ID, err error) error {
	if stderr.As(err, &rpcerr.ErrorType{}) {
		return err
	}

	if stderr.Is(err, block.ErrNotEnoughSpace) {
		log.Debug(ctx, "Append failed: not enough space. Mark segment is full.", map[string]interface{}{
			"blockID": blockID,
		})
		// TODO: optimize this to async from sync
		if err = s.markSegmentIsFull(ctx, blockID); err != nil {
			return err
		}
		return errors.ErrSegmentNotEnoughSpace
	} else if stderr.Is(err, block.ErrFull) {
		log.Debug(ctx, "Append failed: block is full.", map[string]interface{}{
			"blockID": blockID,
		})
		return errors.ErrSegmentFull
	}

	log.Warning(ctx, "Append failed.", map[string]interface{}{
		"blockID":    blockID,
		log.KeyError: err,
	})
	return errors.ErrInternal.WithMessage("write to storage failed").Wrap(err)
}

func (s *server) markSegmentIsFull(ctx context.Context, blockID vanus.ID) error {
	var b block.Block
	if v, ok := s.blocks.Load(blockID); ok {
		b, _ = v.(block.Block)
	} else {
		return fmt.Errorf("the SegmentBlock does not exist")
	}

	// TODO(james.yin): close Appender
	// writer, _ := bl.(block.Writer)
	// if err := writer.CloseWrite(ctx); err != nil {
	// 	return err
	// }

	// report to controller immediately
	_, err := s.cc.reportSegmentBlockIsFull(ctx, &ctrlpb.SegmentHeartbeatRequest{
		ServerId: s.id.Uint64(),
		VolumeId: s.volumeID.Uint64(),
		HealthInfo: []*metapb.SegmentHealthInfo{
			b.HealthInfo(),
		},
		ReportTime: util.FormatTime(time.Now()),
	})
	return err
}

func (s *server) ReadFromBlock(ctx context.Context, id vanus.ID, off int, num int) ([]*cepb.CloudEvent, error) {
	if err := s.checkState(); err != nil {
		return nil, err
	}

	var reader block.Reader
	if v, ok := s.readers.Load(id); ok {
		reader, _ = v.(block.Reader)
	} else {
		return nil, errors.ErrResourceNotFound.WithMessage(
			"the segment doesn't exist on this server")
	}

	entries, err := reader.Read(ctx, off, num)
	if err != nil {
		return nil, err
	}

	events := make([]*cepb.CloudEvent, len(entries))
	for i, entry := range entries {
		event := &cepb.CloudEvent{}
		if err2 := proto.Unmarshal(entry.Payload, event); err2 != nil {
			return nil, errors.ErrInternal.WithMessage(
				"unmarshall data to event failed").Wrap(err2)
		}
		if event.Attributes == nil {
			event.Attributes = make(map[string]*cepb.CloudEventAttributeValue, 1)
		}
		event.Attributes[segpb.XVanusBlockOffset] = &cepb.CloudEventAttributeValue{
			Attr: &cepb.CloudEventAttributeValue_CeInteger{
				CeInteger: int32(entry.Index),
			},
		}
		events[i] = event
	}

	return events, nil
}

func (s *server) checkState() error {
	if s.state != primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't ready to work, current state: %s", s.state))
	}
	return nil
}
