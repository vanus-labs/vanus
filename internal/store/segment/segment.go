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
	v1 "cloudevents.io/genproto/v1"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	"github.com/linkall-labs/vanus/internal/util"
	errutil "github.com/linkall-labs/vanus/internal/util/errors"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	segmentServerDebugModeFlagENV = "SEGMENT_SERVER_DEBUG_MODE"
)

func NewSegmentServer(localAddr, ctrlAddr string, volumeId uint64, stop func()) segment.SegmentServerServer {
	return &segmentServer{
		volumeId:     vanus.NewIDFromUint64(volumeId),
		volumeDir:    "/Users/wenfeng/tmp/data/vanus/volume-1",
		ctrlAddress:  ctrlAddr,
		localAddress: localAddr,
		stopCallback: stop,
		closeCh:      make(chan struct{}, 0),
		state:        primitive.ServerStateCreated,
		events:       make([]*v1.CloudEvent, 0),
		credentials:  insecure.NewCredentials(),
	}
}

type segmentServer struct {
	id                  vanus.ID
	volumeId            vanus.ID
	volumeDir           string
	ctrlAddress         string
	localAddress        string
	stopCallback        func()
	closeCh             chan struct{}
	state               primitive.ServerState
	events              []*v1.CloudEvent
	ctrlGrpcConn        *grpc.ClientConn
	ctrlClient          ctrl.SegmentControllerClient
	credentials         credentials.TransportCredentials
	segmentBlockMap     sync.Map
	segmentBlockWriters sync.Map
	segmentBlockReaders sync.Map
	isDebugMode         bool
}

func (s *segmentServer) Initialize(ctx context.Context) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(s.credentials))
	conn, err := grpc.Dial(s.ctrlAddress, opts...)
	if err != nil {
		return err
	}
	s.ctrlGrpcConn = conn

	// TODO optimize this, because the implementation assumes under storage is linux file system
	var files []string
	if strings.ToLower(os.Getenv(segmentServerDebugModeFlagENV)) != "true" {
		s.ctrlClient = ctrl.NewSegmentControllerClient(conn)
		res, err := s.ctrlClient.RegisterSegmentServer(context.Background(), &ctrl.RegisterSegmentServerRequest{
			Address:  s.localAddress,
			VolumeId: s.volumeId.Uint64(),
		})
		if err != nil {
			return err
		}
		s.id = vanus.NewIDFromUint64(res.ServerId)
		if len(res.SegmentBlocks) > 0 {
			for k := range res.SegmentBlocks {
				files = append(files, filepath.Join(s.volumeDir, k))
			}
		}
		if err != nil {
			return err
		}
	} else {
		log.Info(ctx, "the segment server debug mode enabled", nil)
		s.id = vanus.NewID()
		s.isDebugMode = true
		_files, err := filepath.Glob(filepath.Join(s.volumeDir, "*"))
		if err != nil {
			return err
		}
		files = _files
	}

	if err = s.initSegmentBlock(ctx, files); err != nil {
		return err
	}
	s.state = primitive.ServerStateStarted
	return err
}

func (s *segmentServer) Start(ctx context.Context,
	req *segment.StartSegmentServerRequest) (*segment.StartSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if s.state != primitive.ServerStateStarted {
		return nil, errors.ErrServiceState.WithMessage("start failed, server state is not created")
	}
	if err := s.start(ctx); err != nil {
		return nil, errors.ErrInternal.WithMessage("start heartbeat task failed")
	}
	s.state = primitive.ServerStateRunning
	return &segment.StartSegmentServerResponse{}, nil
}

func (s *segmentServer) Stop(ctx context.Context,
	req *segment.StopSegmentServerRequest) (*segment.StopSegmentServerResponse, error) {
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
	return &segment.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) CreateBlock(ctx context.Context,
	req *segment.CreateBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	_, exist := s.segmentBlockMap.Load(req.Id)
	if exist {
		return nil, errors.ErrResourceAlreadyExist.WithMessage("the segment has already exist")
	}
	path := s.generateNewSegmentBlockPath(vanus.NewIDFromUint64(req.Id))
	segmentBlock, err := block.CreateFileSegmentBlock(ctx, vanus.NewIDFromUint64(req.Id), path, req.Size)
	if err != nil {
		return nil, err
	}

	s.segmentBlockMap.Store(req.Id, segmentBlock)
	s.segmentBlockWriters.Store(req.Id, segmentBlock)
	s.segmentBlockReaders.Store(req.Id, segmentBlock)
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) RemoveBlock(ctx context.Context,
	req *segment.RemoveBlockRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// ActivateSegment mark a block ready to using and preparing to initializing a replica group
func (s *segmentServer) ActivateSegment(ctx context.Context,
	req *segment.ActivateSegmentRequest) (*segment.ActivateSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &segment.ActivateSegmentResponse{}, nil
}

// InactivateSegment mark a block ready to be removed
func (s *segmentServer) InactivateSegment(ctx context.Context,
	req *segment.InactivateSegmentRequest) (*segment.InactivateSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &segment.InactivateSegmentResponse{}, nil
}

func (s *segmentServer) GetBlockInfo(ctx context.Context,
	req *segment.GetBlockInfoRequest) (*segment.GetBlockInfoResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	return &segment.GetBlockInfoResponse{}, nil
}

func (s *segmentServer) AppendToBlock(ctx context.Context,
	req *segment.AppendToSegmentRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	if req.Events == nil || len(req.Events.Events) == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("event list is empty")
	}

	events := req.GetEvents().Events
	v, exist := s.segmentBlockWriters.Load(req.SegmentId)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist")
	}

	writer := v.(block.SegmentBlockWriter)
	if !writer.IsAppendable() {
		if writer.(block.SegmentBlock).IsFull() {
			return nil, errors.ErrSegmentNoEnoughCapacity
		}
		return nil, errors.ErrInternal.WithMessage("the segment can not be appended")
	}

	entities := make([]*codec.StoredEntry, len(events))
	for idx := range events {
		evt := events[idx]
		payload, err := proto.Marshal(evt)
		if err != nil {
			return nil, errors.ErrInternal.WithMessage("marshall event failed").Wrap(err)
		}
		entities[idx] = &codec.StoredEntry{
			Length:  int32(len(payload)),
			Payload: payload,
		}
	}

	if err := writer.Append(ctx, entities...); err != nil {
		if err == block.ErrNoEnoughCapacity {
			// TODO optimize this to async from sync
			if err = s.markSegmentIsFull(ctx, vanus.NewIDFromUint64(req.SegmentId)); err != nil {
				return nil, err
			}
			return nil, errors.ErrSegmentNoEnoughCapacity
		}
		return nil, errors.ErrInternal.WithMessage("write to storage failed")
	}
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) ReadFromBlock(ctx context.Context,
	req *segment.ReadFromSegmentRequest) (*segment.ReadFromSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	if err := s.checkoutState(); err != nil {
		return nil, err
	}

	segV, exist := s.segmentBlockMap.Load(req.SegmentId)
	if !exist {
		return nil, errors.ErrResourceNotFound.WithMessage("the segment doesn't exist on this server")
	}

	segBlock := segV.(block.SegmentBlock)
	v, exist := s.segmentBlockReaders.Load(req.SegmentId)
	var reader block.SegmentBlockReader
	if !exist {
		_reader, err := block.OpenFileSegmentBlock(ctx, segBlock.Path())
		if err != nil {
			return nil, err
		}
		reader = _reader
		s.segmentBlockReaders.Store(req.SegmentId, reader)
	} else {
		reader = v.(block.SegmentBlockReader)
	}
	entities, err := reader.Read(ctx, int(req.Offset), int(req.Number))
	if err != nil {
		return nil, err
	}

	events := make([]*v1.CloudEvent, len(entities))
	for idx := range entities {
		v := &v1.CloudEvent{}
		if err := proto.Unmarshal(entities[idx].Payload, v); err != nil {
			return nil, errors.ErrInternal.WithMessage("unmarshall data to event failed").Wrap(err)
		}
		events[idx] = v
	}

	return &segment.ReadFromSegmentResponse{
		Events: &v1.CloudEventBatch{Events: events},
	}, nil
}

func (s *segmentServer) startHeartBeatTask() error {
	if s.isDebugMode {
		return nil
	}
	stream, err := s.ctrlClient.SegmentHeartbeat(context.Background())
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		ctx := context.Background()
	LOOP:
		for {
			select {
			case <-s.closeCh:
				break LOOP
			case <-ticker.C:
				infos := make([]*meta.SegmentHealthInfo, 0)
				s.segmentBlockMap.Range(func(key, value interface{}) bool {
					infos = append(infos, value.(block.SegmentBlock).HealthInfo())
					return true
				})
				if err = stream.Send(&ctrl.SegmentHeartbeatRequest{
					ServerId:   s.id.Uint64(),
					VolumeId:   s.volumeId.Uint64(),
					HealthInfo: infos,
					ReportTime: util.FormatTime(time.Now()),
					ServerAddr: s.localAddress,
				}); err != nil {
					if err == io.EOF {
						log.Warning(ctx, "send heartbeat to controller failed, connection lost. try to reconnecting",
							nil)
					} else {
						log.Warning(ctx, "send heartbeat to controller failed, try to reconnecting", map[string]interface{}{
							log.KeyError: err,
						})
					}

					for err != nil {
						stream, err = s.ctrlClient.SegmentHeartbeat(context.Background())
						if err != nil {
							log.Error(ctx, "reconnect to controller failed, retry after 3s", map[string]interface{}{
								log.KeyError: err,
							})
						}
						time.Sleep(3 * time.Second)
					}
					log.Info(ctx, "reconnected to controller", nil)
				}
			}
		}
		if _, err = stream.CloseAndRecv(); err != nil {
			log.Warning(ctx, "close gRPC stream error", map[string]interface{}{
				log.KeyError: err,
			})
		}
		ticker.Stop()
	}()
	return nil
}

func (s *segmentServer) generateNewSegmentBlockPath(id vanus.ID) string {
	return filepath.Join(s.volumeDir, id.String())
}

func (s *segmentServer) start(ctx context.Context) error {
	wg := sync.WaitGroup{}

	var err error
	s.segmentBlockMap.Range(func(key, value interface{}) bool {
		wg.Add(1)
		go func(segBlock block.SegmentBlock) {
			if _err := segBlock.Initialize(ctx); err != nil {
				err = errutil.Chain(err, _err)
			}
			s.segmentBlockWriters.Store(segBlock.SegmentBlockID(), segBlock)
			s.segmentBlockReaders.Store(segBlock.SegmentBlockID(), segBlock)
			wg.Done()
		}(value.(block.SegmentBlock))
		return true
	})
	wg.Wait()

	if err := s.startHeartBeatTask(); err != nil {
		return err
	}
	return nil
}

func (s *segmentServer) stop(ctx context.Context) error {
	err := s.ctrlGrpcConn.Close()
	if err != nil {
		log.Warning(ctx, "close gRPC connection of controller failed", map[string]interface{}{
			log.KeyError: err,
		})
	}

	wg := sync.WaitGroup{}

	{
		wg.Add(1)
		go func() {
			s.waitAllAppendRequestCompleted(ctx)
			s.segmentBlockWriters.Range(func(key, value interface{}) bool {
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
			s.segmentBlockReaders.Range(func(key, value interface{}) bool {
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
	bl, exist := s.segmentBlockMap.Load(segId)
	if !exist {
		return fmt.Errorf("the SegmentBlock does not exist")
	}

	if err := bl.(block.SegmentBlockWriter).CloseWrite(ctx); err != nil {
		return err
	}

	// report to controller immediately
	_, err := s.ctrlClient.ReportSegmentBlockIsFull(ctx, &ctrl.SegmentHeartbeatRequest{
		ServerId: s.id.Uint64(),
		VolumeId: s.volumeId.Uint64(),
		HealthInfo: []*meta.SegmentHealthInfo{
			bl.(block.SegmentBlock).HealthInfo(),
		},
		ReportTime: util.FormatTime(time.Now()),
	})
	return err
}

func (s *segmentServer) waitAllAppendRequestCompleted(ctx context.Context) {}

func (s *segmentServer) waitAllReadRequestCompleted(ctx context.Context) {}

func (s *segmentServer) initSegmentBlock(ctx context.Context, files []string) error {
	for idx := range files {
		sb, err := block.OpenFileSegmentBlock(ctx, files[idx])
		if err != nil {
			return err
		}
		s.segmentBlockMap.Store(sb.SegmentBlockID(), sb)
	}
	return nil
}

func (s *segmentServer) checkoutState() error {
	if s.state == primitive.ServerStateRunning {
		return errors.ErrServiceState.WithMessage(fmt.Sprintf(
			"the server isn't ready to work, current state:%s", s.state))
	}
	return nil
}
