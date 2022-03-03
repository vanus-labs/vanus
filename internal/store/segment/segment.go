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
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	segmentServerDebugModeFlagENV = "SEGMENT_SERVER_DEBUG_MODE"
)

func NewSegmentServer(localAddr, ctrlAddr, volumeId string, stop func()) segment.SegmentServerServer {
	return &segmentServer{
		volumeId:        volumeId,
		volumeDir:       "/Users/wenfeng/tmp/data/vanus/volume-1",
		ctrlAddress:     ctrlAddr,
		localAddress:    localAddr,
		stopCallback:    stop,
		closeCh:         make(chan struct{}, 0),
		state:           primitive.ServerStateCreated,
		events:          make([]*v1.CloudEvent, 0),
		credentials:     insecure.NewCredentials(),
		segmentBlockMap: map[string]block.SegmentBlock{},
	}
}

type segmentServer struct {
	id                  string
	volumeId            string
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
	segmentBlockMap     map[string]block.SegmentBlock
	segmentBlockWriters sync.Map
	segmentBlockReaders sync.Map
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
			VolumeId: s.volumeId,
		})
		s.id = res.ServerId
		if len(res.SegmentBlocks) > 0 {
			for idx := range res.SegmentBlocks {
				files = append(files, res.SegmentBlocks[idx])
			}
		}
		if err != nil {
			return err
		}
	} else {
		log.Info("the segment server debug mode enabled", nil)
		s.id = "test-server-1"
		_files, err := filepath.Glob(filepath.Join(s.volumeDir, "*"))
		if err != nil {
			return err
		}
		files = _files
	}

	if err = s.initSegmentBlock(ctx, files); err != nil {
		return errors.Chain(fmt.Errorf("repair segment blocks failed under debug mode"), err)
	}
	s.state = primitive.ServerStateStarted
	return err
}

func (s *segmentServer) Start(ctx context.Context,
	req *segment.StartSegmentServerRequest) (*segment.StartSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if s.state != primitive.ServerStateStarted {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"start failed, server state is not created")
	}
	if err := s.start(ctx); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"start heartbeat task failed", err)
	}
	s.state = primitive.ServerStateRunning
	return &segment.StartSegmentServerResponse{}, nil
}

func (s *segmentServer) Stop(ctx context.Context,
	req *segment.StopSegmentServerRequest) (*segment.StopSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if s.state != primitive.ServerStateRunning {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	s.state = primitive.ServerStateStopped
	if err := s.stop(ctx); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"stop server failed", err)
	}

	s.stopCallback()
	return &segment.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) CreateSegmentBlock(ctx context.Context,
	req *segment.CreateSegmentBlockRequest) (*segment.CreateSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	_, exist := s.segmentBlockMap[req.Id]
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"the segment has already exist")
	}
	path := s.generateNewSegmentBlockPath(req.Id)
	segmentBlock, err := block.CreateFileSegmentBlock(ctx, req.Id, path, req.Size)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"create segment block failed", err)
	}

	s.segmentBlockMap[req.Id] = segmentBlock
	s.segmentBlockWriters.Store(req.Id, segmentBlock)
	s.segmentBlockReaders.Store(req.Id, segmentBlock)
	return &segment.CreateSegmentBlockResponse{}, nil
}

func (s *segmentServer) RemoveSegmentBlock(ctx context.Context,
	req *segment.RemoveSegmentBlockRequest) (*segment.RemoveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	return &segment.RemoveSegmentBlockResponse{}, nil
}

// ActiveSegmentBlock mark a block ready to using and preparing to initializing a replica group
func (s *segmentServer) ActiveSegmentBlock(ctx context.Context,
	req *segment.ActiveSegmentBlockRequest) (*segment.ActiveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	return &segment.ActiveSegmentBlockResponse{}, nil
}

// InactiveSegmentBlock mark a block ready to be removed
func (s *segmentServer) InactiveSegmentBlock(ctx context.Context,
	req *segment.InactiveSegmentBlockRequest) (*segment.InactiveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	return &segment.InactiveSegmentBlockResponse{}, nil
}

func (s *segmentServer) GetSegmentBlockInfo(ctx context.Context,
	req *segment.GetSegmentBlockInfoRequest) (*segment.GetSegmentBlockInfoResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}
	return &segment.GetSegmentBlockInfoResponse{}, nil
}

func (s *segmentServer) AppendToSegment(ctx context.Context,
	req *segment.AppendToSegmentRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	if req.Events == nil || len(req.Events.Events) == 0 {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "event list is empty")
	}

	events := req.GetEvents().Events
	v, exist := s.segmentBlockWriters.Load(req.SegmentId)
	if !exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"the segment doesn't exist")
	}

	writer := v.(block.SegmentBlockWriter)
	if !writer.IsAppendable() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the segment can not be appended, isFull: %v",
				writer.(block.SegmentBlock).IsFull())))
	}

	entities := make([]*codec.StoredEntry, len(events))
	for idx := range events {
		evt := events[idx]
		log.Debug("received a event", map[string]interface{}{
			"source": evt.Source,
			"id":     evt.Id,
			"type":   evt.Type,
			"attrs":  evt.Attributes,
			"data":   evt.Data,
		})
		payload, err := proto.Marshal(evt)
		if err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "marshall event failed", err)
		}
		entities[idx] = &codec.StoredEntry{
			Length:  int32(len(payload)),
			Payload: payload,
		}
	}

	if err := writer.Append(ctx, entities...); err != nil {
		if err == block.ErrNoEnoughCapacity {
			// TODO optimize this to async from sync
			if err = s.markSegmentIsFull(ctx, req.SegmentId); err != nil {
				return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
					"mark segment to can not be wrote failed", err)
			}
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "segment no space left", nil)
		}
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "write to storage failed", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) ReadFromSegment(ctx context.Context,
	req *segment.ReadFromSegmentRequest) (*segment.ReadFromSegmentResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	if !s.isServerReadyToWork() {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			errors.Notice(fmt.Sprintf("the server isn't ready to work, current state:%s", s.state)))
	}

	segBlock, exist := s.segmentBlockMap[req.SegmentId]
	if !exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"the segment doesn't exist on this server")
	}

	v, exist := s.segmentBlockReaders.Load(req.SegmentId)
	var reader block.SegmentBlockReader
	if !exist {
		_reader, err := block.OpenFileSegmentBlock(ctx, segBlock.Path())
		if err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
				"open segment failed", err)
		}
		reader = _reader
		s.segmentBlockReaders.Store(req.SegmentId, reader)
	} else {
		reader = v.(block.SegmentBlockReader)
	}
	entities, err := reader.Read(ctx, int(req.Offset), int(req.Number))
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
			"read data from segment failed", err)
	}

	events := make([]*v1.CloudEvent, len(entities))
	for idx := range entities {
		v := &v1.CloudEvent{}
		if err := proto.Unmarshal(entities[idx].Payload, v); err != nil {
			return nil, errors.ConvertGRPCError(errors.NotBeenClassified,
				"unmarshall data to event failed", err)
		}
		events[idx] = v
	}

	return &segment.ReadFromSegmentResponse{
		Events: &v1.CloudEventBatch{Events: events},
	}, nil
}

func (s *segmentServer) startHeartBeatTask() error {
	stream, err := s.ctrlClient.SegmentHeartbeat(context.Background())
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second)
	LOOP:
		for {
			select {
			case <-s.closeCh:
				break LOOP
			case <-ticker.C:
				if err = stream.Send(&ctrl.SegmentHeartbeatRequest{}); err != nil {
					log.Warning("send heartbeat to controller error", map[string]interface{}{
						log.KeyError: err,
					})
				}
			}
		}
		if _, err = stream.CloseAndRecv(); err != nil {
			log.Warning("close gRPC stream error", map[string]interface{}{
				log.KeyError: err,
			})
		}
		ticker.Stop()
	}()
	return nil
}

func (s *segmentServer) generateNewSegmentBlockPath(id string) string {
	return filepath.Join(s.volumeDir, id)
}

func (s *segmentServer) start(ctx context.Context) error {
	wg := sync.WaitGroup{}

	var err error
	for id := range s.segmentBlockMap {
		wg.Add(1)
		go func(segBlock block.SegmentBlock) {
			if _err := segBlock.Initialize(ctx); err != nil {
				err = errors.Chain(err, _err)
			}
			s.segmentBlockWriters.Store(segBlock.SegmentBlockID(), segBlock)
			s.segmentBlockReaders.Store(segBlock.SegmentBlockID(), segBlock)
			wg.Done()
		}(s.segmentBlockMap[id])
	}
	wg.Wait()

	//if err := s.startHeartBeatTask(); err != nil {
	//	return err
	//}
	return nil
}

func (s *segmentServer) stop(ctx context.Context) error {
	err := s.ctrlGrpcConn.Close()
	if err != nil {
		log.Warning("close gRPC connection of controller failed", map[string]interface{}{
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
					err = errors.Chain(err, _err)
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
					err = errors.Chain(err, _err)
				}
				return true
			})
			wg.Done()
		}()
	}

	wg.Wait()
	return err
}

func (s *segmentServer) markSegmentIsFull(ctx context.Context, segId string) error {
	bl := s.segmentBlockMap[segId]
	if bl == nil {
		return fmt.Errorf("the SegmentBlock does not exist")
	}

	// TODO report to Controller
	return bl.CloseWrite(ctx)
}

func (s *segmentServer) isServerReadyToWork() bool {
	return s.state == primitive.ServerStateRunning
}

func (s *segmentServer) waitAllAppendRequestCompleted(ctx context.Context) {}

func (s *segmentServer) waitAllReadRequestCompleted(ctx context.Context) {}

func (s *segmentServer) initSegmentBlock(ctx context.Context, files []string) error {
	for idx := range files {
		sb, err := block.OpenFileSegmentBlock(ctx, files[idx])
		if err != nil {
			return err
		}
		s.segmentBlockMap[sb.SegmentBlockID()] = sb
	}
	return nil
}
