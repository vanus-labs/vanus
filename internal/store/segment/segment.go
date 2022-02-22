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
	"context"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/protobuf/types/known/emptypb"
)

func NewSegmentServer(stop func()) segment.SegmentServerServer {
	return &segmentServer{}
}

type segmentServer struct {
}

func (s *segmentServer) Start(context.Context, *segment.StartSegmentServerRequest) (*segment.StartSegmentServerResponse, error) {
	return nil, nil
}

func (s *segmentServer) Stop(context.Context, *segment.StopSegmentServerRequest) (*segment.StopSegmentServerResponse, error) {
	return nil, nil
}

func (s *segmentServer) CreateSegmentBlock(context.Context, *segment.CreateSegmentBlockRequest) (*segment.CreateSegmentBlockResponse, error) {
	return nil, nil
}

func (s *segmentServer) RemoveSegmentBlock(context.Context, *segment.RemoveSegmentBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (s *segmentServer) GetSegmentBlockInfo(context.Context, *segment.GetSegmentBlockInfoRequest) (*segment.GetSegmentBlockInfoResponse, error) {
	return nil, nil
}

func (s *segmentServer) AppendToSegment(context.Context, *segment.AppendToSegmentRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (s *segmentServer) ReadFromSegment(context.Context, *segment.ReadFromSegmentRequest) (*segment.ReadFromSegmentResponse, error) {
	return nil, nil
}
