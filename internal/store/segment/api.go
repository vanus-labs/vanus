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

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type segmentServer struct {
	srv Server
}

// Make sure segmentServer implements segpb.SegmentServerServer.
var _ segpb.SegmentServerServer = (*segmentServer)(nil)

func (s *segmentServer) Start(
	ctx context.Context, req *segpb.StartSegmentServerRequest,
) (*segpb.StartSegmentServerResponse, error) {
	if err := s.srv.Start(ctx); err != nil {
		return nil, err
	}

	return &segpb.StartSegmentServerResponse{}, nil
}

func (s *segmentServer) Stop(
	ctx context.Context, req *segpb.StopSegmentServerRequest,
) (*segpb.StopSegmentServerResponse, error) {
	if err := s.srv.Stop(ctx); err != nil {
		return nil, err
	}

	return &segpb.StopSegmentServerResponse{}, nil
}

func (s *segmentServer) Status(ctx context.Context, req *emptypb.Empty) (*segpb.StatusResponse, error) {
	return &segpb.StatusResponse{Status: string(s.srv.Status())}, nil
}

func (s *segmentServer) CreateBlock(ctx context.Context, req *segpb.CreateBlockRequest) (*emptypb.Empty, error) {
	blockID := vanus.NewIDFromUint64(req.Id)
	if err := s.srv.CreateBlock(ctx, blockID, req.Size); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *segmentServer) RemoveBlock(ctx context.Context, req *segpb.RemoveBlockRequest) (*emptypb.Empty, error) {
	blockID := vanus.NewIDFromUint64(req.Id)
	if err := s.srv.RemoveBlock(ctx, blockID); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (s *segmentServer) GetBlockInfo(
	ctx context.Context, req *segpb.GetBlockInfoRequest,
) (*segpb.GetBlockInfoResponse, error) {
	// TODO(james.yin): implements GetBlockInfo()
	// if err := s.srv.GetBlockInfo(ctx, 0); err != nil {
	// 	return nil, err
	// }

	return &segpb.GetBlockInfoResponse{}, nil
}

func (s *segmentServer) ActivateSegment(
	ctx context.Context, req *segpb.ActivateSegmentRequest,
) (*segpb.ActivateSegmentResponse, error) {
	logID := vanus.NewIDFromUint64(req.EventLogId)
	segID := vanus.NewIDFromUint64(req.ReplicaGroupId)
	replicas := make(map[vanus.ID]string, len(req.Replicas))
	for id, endpoint := range req.Replicas {
		blockID := vanus.NewIDFromUint64(id)
		replicas[blockID] = endpoint
	}

	if err := s.srv.ActivateSegment(ctx, logID, segID, replicas); err != nil {
		return nil, err
	}

	return &segpb.ActivateSegmentResponse{}, nil
}

func (s *segmentServer) InactivateSegment(
	ctx context.Context, req *segpb.InactivateSegmentRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *segmentServer) AppendToBlock(
	ctx context.Context, req *segpb.AppendToBlockRequest) (*segpb.AppendToBlockResponse, error) {
	blockID := vanus.NewIDFromUint64(req.BlockId)
	events := req.Events.GetEvents()
	offs, err := s.srv.AppendToBlock(ctx, blockID, events)
	if err != nil {
		return nil, err
	}

	return &segpb.AppendToBlockResponse{Offsets: offs}, nil
}

func (s *segmentServer) ReadFromBlock(
	ctx context.Context, req *segpb.ReadFromBlockRequest) (*segpb.ReadFromBlockResponse, error) {
	blockID := vanus.NewIDFromUint64(req.BlockId)
	events, err := s.srv.ReadFromBlock(ctx, blockID, req.Offset, int(req.Number), req.PollingTimeout)
	if err != nil {
		return nil, err
	}

	return &segpb.ReadFromBlockResponse{
		Events: &cepb.CloudEventBatch{Events: events},
	}, nil
}
