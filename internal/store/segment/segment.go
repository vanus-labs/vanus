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
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

func NewSegmentServer(localAddr, ctrlAddr, volumeId string, stop func()) segment.SegmentServerServer {
	return &segmentServer{
		localAddress: localAddr,
		ctrlAddress:  ctrlAddr,
		volumeId:     volumeId,
		credentials:  insecure.NewCredentials(),
		stopCallback: stop,
		closeCh:      make(chan struct{}, 0),
		events:       make([]*v1.CloudEvent, 0),
	}
}

type segmentServer struct {
	id                  string
	volumeId            string
	ctrlAddress         string
	localAddress        string
	stopCallback        func()
	closeCh             chan struct{}
	events              []*v1.CloudEvent
	grpcConn            *grpc.ClientConn
	ctrlClient          ctrl.SegmentControllerClient
	credentials         credentials.TransportCredentials
	segmentBlockMap     map[int64]string
	segmentBlockWriter  map[int64]block.StorageBlockWriter
	segmentBlockReaders map[int64]block.StorageBlockReader
}

func (s *segmentServer) Initialize() error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(s.credentials))
	conn, err := grpc.Dial(s.ctrlAddress, opts...)
	if err != nil {
		return err
	}
	s.grpcConn = conn
	s.ctrlClient = ctrl.NewSegmentControllerClient(conn)
	res, err := s.ctrlClient.RegisterSegmentServer(context.Background(), &ctrl.RegisterSegmentServerRequest{
		Address:  s.localAddress,
		VolumeId: s.volumeId,
	})
	if err != nil {
		return err
	}
	s.id = res.ServerId
	s.segmentBlockMap = res.SegmentBlocks
	return err
}

func (s *segmentServer) Start(ctx context.Context,
	req *segment.StartSegmentServerRequest) (*segment.StartSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if err := s.startHeartBeatTask(); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "start heartbeat task failed", err)
	}
	return nil, nil
}

func (s *segmentServer) Stop(ctx context.Context,
	req *segment.StopSegmentServerRequest) (*segment.StopSegmentServerResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	err := s.grpcConn.Close()
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "close grpc conn failed", err)
	}
	s.stopCallback()
	return &segment.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) CreateSegmentBlock(ctx context.Context,
	req *segment.CreateSegmentBlockRequest) (*segment.CreateSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return nil, nil
}

func (s *segmentServer) RemoveSegmentBlock(ctx context.Context,
	req *segment.RemoveSegmentBlockRequest) (*segment.RemoveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &segment.RemoveSegmentBlockResponse{}, nil
}

// ActiveSegmentBlock mark a block ready to use and build up a replica group
func (s *segmentServer) ActiveSegmentBlock(ctx context.Context,
	req *segment.ActiveSegmentBlockRequest) (*segment.ActiveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &segment.ActiveSegmentBlockResponse{}, nil
}

// InactiveSegmentBlock mark a block ready to be removed
func (s *segmentServer) InactiveSegmentBlock(ctx context.Context,
	req *segment.InactiveSegmentBlockRequest) (*segment.InactiveSegmentBlockResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &segment.InactiveSegmentBlockResponse{}, nil
}

func (s *segmentServer) GetSegmentBlockInfo(ctx context.Context,
	req *segment.GetSegmentBlockInfoRequest) (*segment.GetSegmentBlockInfoResponse, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return &segment.GetSegmentBlockInfoResponse{}, nil
}

func (s *segmentServer) AppendToSegment(ctx context.Context,
	req *segment.AppendToSegmentRequest) (*emptypb.Empty, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	events := req.GetEvents().Events
	for idx := range events {
		evt := events[idx]
		log.Debug("received a event", map[string]interface{}{
			"source": evt.Source,
			"id":     evt.Id,
			"type":   evt.Type,
			"attrs":  evt.Attributes,
			"data":   evt.Data,
		})
		s.events = append(s.events, evt)
	}
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) ReadFromSegment(ctx context.Context,
	req *segment.ReadFromSegmentRequest) (*segment.ReadFromSegmentResponse, error) {
	return &segment.ReadFromSegmentResponse{
		Events: &v1.CloudEventBatch{Events: s.events},
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
