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
	"sync"

	// third-party libraries.
	cepb "cloudevents.io/genproto/v1"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.

	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/errors"
)

const (
	defaultChannelBuffer = 32
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
	ctx context.Context, req *segpb.AppendToBlockRequest,
) (*segpb.AppendToBlockResponse, error) {
	var (
		offsets []int64 = make([]int64, 0)
		err     error
	)
	callbackFunc := func(offs []int64, e error) {
		offsets = offs
		err = e
	}
	blockID := vanus.NewIDFromUint64(req.BlockId)
	events := req.Events.GetEvents()
	s.srv.AppendToBlock(ctx, blockID, events, callbackFunc)
	// offs, err := s.srv.AppendToBlock(ctx, blockID, events)
	// if err != nil {
	// 	return nil, err
	// }
	return &segpb.AppendToBlockResponse{Offsets: offsets}, err
}

func (s *segmentServer) AppendToBlockStream(stream segpb.SegmentServer_AppendToBlockStreamServer) error {
	ctx := context.Background()

	for {
		request, err := stream.Recv()
		if err != nil {
			log.Warning(ctx, "append stream recv failed", map[string]interface{}{
				log.KeyError: err,
			})
			return err
		}

		callbackFunc := func(offsets []int64, err error) {
			errCode := errpb.ErrorCode_SUCCESS
			errMsg := "success"
			if err != nil {
				if errors.Is(err, errors.ErrFull) {
					errCode = err.(*errors.ErrorType).Code
					errMsg = err.(*errors.ErrorType).Message
				} else {
					errCode = errpb.ErrorCode_UNKNOWN
					errMsg = "unknown"
				}
				log.Error(ctx, "append to block failed", map[string]interface{}{
					log.KeyError: err,
				})
			}

			err = stream.Send(&segpb.AppendToBlockStreamResponse{
				ResponseId:   request.RequestId,
				ResponseCode: errCode,
				ResponseMsg:  errMsg,
				Offsets:      offsets,
			})
			if err != nil {
				log.Warning(ctx, "read stream send failed", map[string]interface{}{
					log.KeyError: err,
				})
				return
			}
		}

		go func(stream segpb.SegmentServer_AppendToBlockStreamServer, request *segpb.AppendToBlockStreamRequest) {
			// errCode := errpb.ErrorCode_SUCCESS
			// errMsg := "success"
			s.srv.AppendToBlock(ctx, vanus.ID(request.BlockId), request.Events.Events, callbackFunc)
			// if err != nil {
			// 	if errors.Is(err, errors.ErrFull) {
			// 		errCode = err.(*errors.ErrorType).Code
			// 		errMsg = err.(*errors.ErrorType).Message
			// 	} else {
			// 		errCode = errpb.ErrorCode_UNKNOWN
			// 		errMsg = "unknown"
			// 	}
			// 	log.Error(ctx, "append to block failed", map[string]interface{}{
			// 		log.KeyError: err,
			// 	})
			// }

			// err = stream.Send(&segpb.AppendToBlockStreamResponse{
			// 	ResponseId:   request.RequestId,
			// 	ResponseCode: errCode,
			// 	ResponseMsg:  errMsg,
			// 	Offsets:      offsets,
			// })
			// if err != nil {
			// 	log.Warning(ctx, "read stream send failed", map[string]interface{}{
			// 		log.KeyError: err,
			// 	})
			// 	return
			// }
		}(stream, request)
	}
}

// func (s *segmentServer) AppendToBlockStream(stream segpb.SegmentServer_AppendToBlockStreamServer) error {
// 	var (
// 		err      error
// 		wg       sync.WaitGroup
// 		requestC chan *segpb.AppendToBlockStreamRequest
// 	)
// 	ctx, cancel := context.WithCancel(context.Background())
// 	requestC = make(chan *segpb.AppendToBlockStreamRequest, defaultChannelBuffer)
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				log.Debug(ctx, "context canceled at append stream handler", nil)
// 				return
// 			case request := <-requestC:
// 				log.Error(ctx, "===jk3===", map[string]interface{}{
// 					"RequestId": request.RequestId,
// 					"lenC":      len(requestC),
// 				})
// 				errCode := errpb.ErrorCode_SUCCESS
// 				errMsg := "success"
// 				offsets, err := s.srv.AppendToBlock(ctx, vanus.ID(request.BlockId), request.Events.Events)
// 				if err != nil {
// 					if errors.Is(err, errors.ErrFull) {
// 						errCode = err.(*errors.ErrorType).Code
// 						errMsg = err.(*errors.ErrorType).Message
// 					} else {
// 						errCode = errpb.ErrorCode_UNKNOWN
// 						errMsg = "unknown"
// 					}
// 					log.Error(ctx, "append to block failed", map[string]interface{}{
// 						log.KeyError: err,
// 					})
// 				}

// 				log.Error(ctx, "===jk4===", map[string]interface{}{
// 					"RequestId": request.RequestId,
// 					"lenC":      len(requestC),
// 				})

// 				err = stream.Send(&segpb.AppendToBlockStreamResponse{
// 					ResponseId:   request.RequestId,
// 					ResponseCode: errCode,
// 					ResponseMsg:  errMsg,
// 					Offsets:      offsets,
// 				})
// 				if err != nil {
// 					log.Warning(ctx, "read stream send failed", map[string]interface{}{
// 						log.KeyError: err,
// 					})
// 				}
// 				log.Error(ctx, "===jk5===", map[string]interface{}{
// 					"RequestId": request.RequestId,
// 					"lenC":      len(requestC),
// 				})
// 			}
// 		}
// 	}()
// 	for {
// 		request, err := stream.Recv()
// 		if err != nil {
// 			log.Warning(ctx, "append stream recv failed", map[string]interface{}{
// 				log.KeyError: err,
// 			})
// 			cancel()
// 			break
// 		}
// 		log.Error(ctx, "===jk1===", map[string]interface{}{
// 			"RequestId": request.RequestId,
// 			"lenC":      len(requestC),
// 		})
// 		requestC <- request
// 		log.Error(ctx, "===jk2===", map[string]interface{}{
// 			"RequestId": request.RequestId,
// 			"lenC":      len(requestC),
// 		})
// 	}
// 	wg.Wait()
// 	close(requestC)
// 	return err
// }

func (s *segmentServer) ReadFromBlock(
	ctx context.Context, req *segpb.ReadFromBlockRequest,
) (*segpb.ReadFromBlockResponse, error) {
	blockID := vanus.NewIDFromUint64(req.BlockId)
	events, err := s.srv.ReadFromBlock(ctx, blockID, req.Offset, int(req.Number), req.PollingTimeoutInMillisecond)
	if err != nil {
		return nil, err
	}

	return &segpb.ReadFromBlockResponse{
		Events: &cepb.CloudEventBatch{Events: events},
	}, nil
}

func (s *segmentServer) ReadFromBlockStream(stream segpb.SegmentServer_ReadFromBlockStreamServer) error {
	var (
		err      error
		wg       sync.WaitGroup
		requestC chan *segpb.ReadFromBlockStreamRequest
	)
	ctx, cancel := context.WithCancel(context.Background())
	requestC = make(chan *segpb.ReadFromBlockStreamRequest, defaultChannelBuffer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Debug(ctx, "context canceled at read stream handler", nil)
				return
			case request := <-requestC:
				errCode := errpb.ErrorCode_SUCCESS
				errMsg := "success"
				blockID := vanus.NewIDFromUint64(request.BlockId)
				events, err := s.srv.ReadFromBlock(ctx, blockID, request.Offset, int(request.Number), request.PollingTimeoutInMillisecond)
				if err != nil {
					errCode = errpb.ErrorCode_UNKNOWN
					errMsg = "unknown"
					log.Error(ctx, "read from block failed", map[string]interface{}{
						log.KeyError: err,
					})
				}

				err = stream.Send(&segpb.ReadFromBlockStreamResponse{
					ResponseId:   request.RequestId,
					ResponseCode: errCode,
					ResponseMsg:  errMsg,
					Events:       &cepb.CloudEventBatch{Events: events},
				})
				if err != nil {
					log.Error(ctx, "read stream send failed", map[string]interface{}{
						log.KeyError: err,
					})
				}
			}
		}
	}()
	for {
		request, err := stream.Recv()
		if err != nil {
			log.Warning(ctx, "read stream recv failed", map[string]interface{}{
				log.KeyError: err,
			})
			cancel()
			break
		}
		requestC <- request
	}
	wg.Wait()
	close(requestC)
	return err
}

func (s *segmentServer) LookupOffsetInBlock(
	ctx context.Context, req *segpb.LookupOffsetInBlockRequest,
) (*segpb.LookupOffsetInBlockResponse, error) {
	blockID := vanus.NewIDFromUint64(req.BlockId)
	off, err := s.srv.LookupOffsetInBlock(ctx, blockID, req.Stime)
	if err != nil {
		return nil, err
	}

	return &segpb.LookupOffsetInBlockResponse{Offset: off}, nil
}
