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
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/observability/log"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

func NewSegmentServer(ctrlAddr string, stop func()) segment.SegmentServerServer {
	return &segmentServer{
		id:           uuid.NewString(),
		ctrlAddress:  ctrlAddr,
		stopCallback: stop,
		localAddress: "127.0.0.1:11811",
		events:       make([]*v1.CloudEvent, 0),
	}
}

type segmentServer struct {
	id           string
	localAddress string
	ctrlAddress  string
	ctrlClient   ctrl.SegmentControllerClient
	grpcConn     *grpc.ClientConn
	closeCh      chan struct{}
	stopCallback func()
	events       []*v1.CloudEvent
}

func (s *segmentServer) Initialize() error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(s.ctrlAddress, opts...)
	if err != nil {
		return err
	}
	s.grpcConn = conn
	s.ctrlClient = ctrl.NewSegmentControllerClient(conn)
	_, err = s.ctrlClient.RegisterSegmentServer(context.Background(), &ctrl.RegisterSegmentServerRequest{
		Address: s.localAddress,
	})
	return err
}

func (s *segmentServer) Start(ctx context.Context,
	req *segment.StartSegmentServerRequest) (*segment.StartSegmentServerResponse, error) {
	if err := s.startHeartBeatTask(); err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "start heartbeat task failed", err)
	}
	return nil, nil
}

func (s *segmentServer) Stop(ctx context.Context,
	req *segment.StopSegmentServerRequest) (*segment.StopSegmentServerResponse, error) {
	err := s.grpcConn.Close()
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "close grpc conn failed", err)
	}
	s.stopCallback()
	return &segment.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) CreateSegmentBlock(ctx context.Context,
	req *segment.CreateSegmentBlockRequest) (*segment.CreateSegmentBlockResponse, error) {
	return nil, nil
}

func (s *segmentServer) RemoveSegmentBlock(ctx context.Context,
	req *segment.RemoveSegmentBlockRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) GetSegmentBlockInfo(ctx context.Context,
	req *segment.GetSegmentBlockInfoRequest) (*segment.GetSegmentBlockInfoResponse, error) {
	return &segment.GetSegmentBlockInfoResponse{}, nil
}

func (s *segmentServer) AppendToSegment(ctx context.Context,
	req *segment.AppendToSegmentRequest) (*emptypb.Empty, error) {
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
