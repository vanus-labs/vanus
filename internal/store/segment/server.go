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
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"

	// first-party libraries.
	"github.com/linkall-labs/vanus/pkg/util"

	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/linkall-labs/vanus/internal/store/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/proto"

	// first-party libraries.
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/linkall-labs/vanus/pkg/controller"
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
	"github.com/linkall-labs/vanus/internal/store/block/raft"
	"github.com/linkall-labs/vanus/internal/store/meta"
	ceconv "github.com/linkall-labs/vanus/internal/store/schema/ce/convert"
	"github.com/linkall-labs/vanus/internal/store/vsb"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
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

	AppendToBlock(ctx context.Context, id vanus.ID, events []*cepb.CloudEvent) ([]int64, error)
	ReadFromBlock(ctx context.Context, id vanus.ID, seq int64, num int, pollingTimeout uint32) ([]*cepb.CloudEvent, error)
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
		volumeIDStr:  strconv.FormatUint(cfg.Volume.ID.Uint64(), 10),
		resolver:     resolver,
		host:         host,
		ctrlAddress:  cfg.ControllerAddresses,
		credentials:  insecure.NewCredentials(),
		leaderc:      make(chan leaderInfo, defaultLeaderInfoBufferSize),
		closec:       make(chan struct{}),
		pm:           &pollingMgr{},
		tracer:       tracing.NewTracer("segment.server", trace.SpanKindServer),
	}

	srv.cc = controller.NewSegmentClient(cfg.ControllerAddresses, srv.credentials)
	return srv
}

type leaderInfo struct {
	leader vanus.ID
	term   uint64
}

type server struct {
	replicas sync.Map // vanus.ID, Replica

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

	volumeID    vanus.ID
	volumeIDStr string
	volumeDir   string

	ctrlAddress []string
	credentials credentials.TransportCredentials
	cc          ctrlpb.SegmentControllerClient
	leaderc     chan leaderInfo

	grpcSrv *grpc.Server
	closec  chan struct{}

	pm     pollingManager
	tracer *tracing.Tracer
}

// Make sure server implements Server.
var _ Server = (*server)(nil)

func (s *server) Serve(lis net.Listener) error {
	segSrv := &segmentServer{
		srv: s,
	}

	raftSrv := transport.NewServer(s.host)
	srv := grpc.NewServer(
		grpc.InTapHandle(s.preGrpcStream),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
			errinterceptor.StreamServerInterceptor(),
			otelgrpc.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(),
			errinterceptor.UnaryServerInterceptor(),
			otelgrpc.UnaryServerInterceptor(
				otelgrpc.WithPropagators(propagation.TraceContext{}),
			),
		),
	)
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
	if err := s.loadEngine(ctx); err != nil {
		return err
	}

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

func (s *server) loadEngine(ctx context.Context) error {
	// TODO(james.yin): how to organize engine?
	return vsb.Initialize(filepath.Join(s.cfg.Volume.Dir, "block"),
		block.ArchivedCallback(s.onBlockArchived))
}

func (s *server) reconcileBlocks(ctx context.Context) error {
	// TODO(james.yin): Fetch block information in volume from controller, and make state up to date.
	return nil
}

func (s *server) registerSelf(ctx context.Context) error {
	// TODO(james.yin): pass information of blocks.
	res, err := s.cc.RegisterSegmentServer(ctx, &ctrlpb.RegisterSegmentServerRequest{
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
			// Don't use address to compare.
			if blockpb.VolumeID == s.volumeID.Uint64() {
				if myID != 0 {
					// FIXME(james.yin): multiple blocks of same segment in this server.
					log.Warning(ctx, "Multiple blocks of the same segment in this server.", map[string]interface{}{
						"block_id":   blockID,
						"other":      myID,
						"segment_id": segmentpb.Id,
						"volume_id":  s.volumeID,
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
				log.Info(ctx, "Block is offline.", map[string]interface{}{
					"block_id":    blockID,
					"segment_id":  segmentpb.Id,
					"eventlog_id": segmentpb.EventLogId,
					"volume_id":   blockpb.VolumeID,
				})
				continue
			}
		}
		s.resolver.Register(blockID, blockpb.Endpoint) //nolint:contextcheck // wrong advice
	}
}

func (s *server) Start(ctx context.Context) error {
	ctx, span := s.tracer.Start(ctx, "Start")
	defer span.End()

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

	return s.runHeartbeat(ctx)
}

func (s *server) runHeartbeat(_ context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-s.closec:
				cancel()
				return
			case info := <-s.leaderc:
				// TODO(james.yin): move to other goroutine.
				req := &ctrlpb.ReportSegmentLeaderRequest{
					LeaderId: info.leader.Uint64(),
					Term:     info.term,
				}
				if _, err := s.cc.ReportSegmentLeader(context.Background(), req); err != nil {
					log.Debug(ctx, "Report segment leader to controller failed.", map[string]interface{}{
						"leader":     info.leader,
						"term":       info.term,
						log.KeyError: err,
					})
				}
			}
		}
	}()

	f := func() interface{} {
		infos := make([]*metapb.SegmentHealthInfo, 0)
		s.replicas.Range(func(key, value interface{}) bool {
			b, _ := value.(Replica)
			infos = append(infos, b.Status())
			return true
		})
		return &ctrlpb.SegmentHeartbeatRequest{
			ServerId:   s.id.Uint64(),
			VolumeId:   s.volumeID.Uint64(),
			HealthInfo: infos,
			ReportTime: util.FormatTime(time.Now()),
			ServerAddr: s.localAddress,
		}
	}

	return controller.RegisterHeartbeat(ctx, time.Second, s.cc, f)
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
	ctx, span := s.tracer.Start(ctx, "Stop")
	defer span.End()
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
	// Close all blocks.
	s.replicas.Range(func(key, value interface{}) bool {
		b, _ := value.(Replica)
		_ = b.Close(ctx)
		return true
	})

	// Close WAL, metaStore, offsetStore.
	s.wal.Close()
	s.offsetStore.Close()
	// Make sure WAL is closed before close metaStore.
	s.wal.Wait()
	s.metaStore.Close(ctx)

	// Stop heartbeat task, etc.
	close(s.closec)

	// Close grpc connections for raft.
	s.host.Stop()

	if closer, ok := s.cc.(io.Closer); ok {
		_ = closer.Close()
	}

	return nil
}

func (s *server) Status() primitive.ServerState {
	return s.state
}

func (s *server) CreateBlock(ctx context.Context, id vanus.ID, size int64) error {
	ctx, span := s.tracer.Start(ctx, "CreateBlock")
	defer span.End()

	if id == 0 {
		log.Warning(ctx, "Can not create block with id(0).", nil)
		return errors.ErrInvalidRequest.WithMessage("can not create block with id(0)")
	}

	if err := s.checkState(); err != nil {
		return err
	}

	log.Info(ctx, "Create block.", map[string]interface{}{
		"block_id": id,
		"size":     size,
	})

	b, err := s.createBlock(ctx, id, size)
	if err != nil {
		if stderr.Is(err, os.ErrExist) {
			return errors.ErrResourceAlreadyExist.WithMessage("the block has already exist")
		}
		return errors.ErrInternal.Wrap(err)
	}

	if _, exist := s.replicas.LoadOrStore(id, b); exist {
		// TODO(james.yin): release resources of block.
		return errors.ErrResourceAlreadyExist.WithMessage("the block has already exist")
	}

	// TODO(james.yin): open replica.

	return nil
}

func (s *server) RemoveBlock(ctx context.Context, blockID vanus.ID) error {
	ctx, span := s.tracer.Start(ctx, "RemoveBlock")
	defer span.End()

	if err := s.checkState(); err != nil {
		return err
	}

	v, exist := s.replicas.LoadAndDelete(blockID)
	if !exist {
		return errors.ErrResourceNotFound.WithMessage("the block not found")
	}

	b, _ := v.(Replica)
	// TODO(james.yin): s.host.Unregister
	if err := b.Delete(ctx); err != nil {
		return err
	}

	// FIXME(james.yin): more info.
	log.Info(ctx, "The block has been deleted.", map[string]interface{}{
		"block_id": b.ID(),
		// "path":     blk.Path(),
		// "metadata": blk.HealthInfo().String(),
	})

	return nil
}

// TODO(james.yin): implements GetBlockInfo.
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
	ctx, span := s.tracer.Start(ctx, "ActivateSegment")
	defer span.End()

	if err := s.checkState(); err != nil {
		return err
	}

	if len(replicas) == 0 {
		log.Warning(ctx, "Replicas can not be empty.", map[string]interface{}{
			"segment_id":  segID,
			"eventlog_id": logID,
		})
		return nil
	}

	log.Info(ctx, "Activate segment.", map[string]interface{}{
		"replicas":    replicas,
		"segment_id":  segID,
		"eventlog_id": logID,
	})

	var myID vanus.ID
	peers := make([]raft.Peer, 0, len(replicas))
	for blockID, endpoint := range replicas {
		peer := raft.Peer{
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

	v, ok := s.replicas.Load(myID)
	if !ok {
		return errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
	}

	// Register peers.
	for i := range peers {
		peer := &peers[i]
		s.resolver.Register(peer.ID.Uint64(), peer.Endpoint) //nolint:contextcheck // wrong advice
	}

	log.Info(ctx, "Bootstrap replica.", map[string]interface{}{
		"block_id": myID,
		"peers":    peers,
	})

	// Bootstrap raft.
	b, _ := v.(Replica)
	if err := b.Bootstrap(ctx, peers); err != nil {
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

func (s *server) AppendToBlock(ctx context.Context, id vanus.ID, events []*cepb.CloudEvent) ([]int64, error) {
	ctx, span := s.tracer.Start(ctx, "AppendToBlock")
	defer span.End()

	if len(events) == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("event list is empty")
	}

	if err := s.checkState(); err != nil {
		return nil, err
	}

	var b Replica
	if v, ok := s.replicas.Load(id); ok {
		b, _ = v.(Replica)
	} else {
		return nil, errors.ErrResourceNotFound.WithMessage("the block doesn't exist")
	}

	var size int
	entries := make([]block.Entry, len(events))
	for i, event := range events {
		entries[i] = ceconv.ToEntry(event)
		size += proto.Size(event)
	}

	metrics.WriteTPSCounterVec.WithLabelValues(s.volumeIDStr, b.IDStr()).Add(float64(len(events)))
	metrics.WriteThroughputCounterVec.WithLabelValues(s.volumeIDStr, b.IDStr()).Add(float64(size))

	seqs, err := b.Append(ctx, entries...)
	if err != nil {
		return nil, s.processAppendError(ctx, b, err)
	}

	// TODO(weihe.yin) make this method deep to code
	s.pm.NewMessageArrived(id)

	return seqs, nil
}

func (s *server) processAppendError(ctx context.Context, b Replica, err error) error {
	if stderr.As(err, &rpcerr.ErrorType{}) {
		return err
	}

	if stderr.Is(err, block.ErrFull) {
		log.Debug(ctx, "Append failed: block is full.", map[string]interface{}{
			"block_id": b.ID(),
		})
		return errors.ErrSegmentFull
	}

	log.Warning(ctx, "Append failed.", map[string]interface{}{
		"block_id":   b.ID(),
		log.KeyError: err,
	})
	return errors.ErrInternal.WithMessage("write to storage failed").Wrap(err)
}

func (s *server) onBlockArchived(stat block.Statistics) {
	id := stat.ID

	log.Debug(context.Background(), "Block is full.", map[string]interface{}{
		"block_id": id,
	})

	// FIXME(james.yin): leader info.
	info := &metapb.SegmentHealthInfo{
		Id:                 id.Uint64(),
		Capacity:           int64(stat.Capacity),
		Size:               int64(stat.EntrySize),
		EventNumber:        int32(stat.EntryNum),
		IsFull:             stat.Archived,
		FirstEventBornTime: stat.FirstEntryStime,
	}
	if stat.Archived {
		info.LastEventBornTime = stat.LastEntryStime
	}

	// report to controller
	go func() {
		_, _ = s.cc.ReportSegmentBlockIsFull(context.Background(), &ctrlpb.SegmentHeartbeatRequest{
			ServerId:   s.id.Uint64(),
			VolumeId:   s.volumeID.Uint64(),
			HealthInfo: []*metapb.SegmentHealthInfo{info},
			ReportTime: util.FormatTime(time.Now()),
			ServerAddr: s.localAddress,
		})
	}()
}

// ReadFromBlock returns at most num events from seq in Block id.
func (s *server) ReadFromBlock(
	ctx context.Context, id vanus.ID, seq int64, num int, pollingTimeout uint32,
) ([]*cepb.CloudEvent, error) {
	ctx, span := s.tracer.Start(ctx, "ReadFromBlock")
	defer span.End()

	if err := s.checkState(); err != nil {
		return nil, err
	}

	var b Replica
	if v, ok := s.replicas.Load(id); ok {
		b, _ = v.(Replica)
	} else {
		return nil, errors.ErrResourceNotFound.WithMessage(
			"the segment doesn't exist on this server")
	}

	if events, err := s.readEvents(ctx, b, seq, num); err == nil {
		return events, nil
	} else if !stderr.Is(err, block.ErrOnEnd) || pollingTimeout == 0 {
		return nil, err
	}

	doneC := s.pm.Add(ctx, id)
	if doneC == nil {
		return nil, block.ErrOnEnd
	}

	t := time.NewTimer(time.Duration(pollingTimeout) * time.Millisecond)
	defer t.Stop()

	select {
	case <-doneC:
		// FIXME(james.yin) It can't read message immediately because of async apply.
		return s.readEvents(ctx, b, seq, num)
	case <-t.C:
		return nil, block.ErrOnEnd
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *server) readEvents(ctx context.Context, b Replica, seq int64, num int) ([]*cepb.CloudEvent, error) {
	entries, err := b.Read(ctx, seq, num)
	if err != nil {
		return nil, err
	}

	var size int
	events := make([]*cepb.CloudEvent, len(entries))
	for i, entry := range entries {
		event := ceconv.ToPb(entry)
		events[i] = event
		size += proto.Size(event)
	}

	metrics.ReadTPSCounterVec.WithLabelValues(s.volumeIDStr, b.IDStr()).Add(float64(len(events)))
	metrics.ReadThroughputCounterVec.WithLabelValues(s.volumeIDStr, b.IDStr()).Add(float64(size))

	return events, nil
}

func (s *server) checkState() error {
	if s.state != primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't ready to work, current state: %s", s.state))
	}
	return nil
}
