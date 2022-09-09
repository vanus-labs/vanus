// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	// standard libraries
	"context"
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"strings"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// first-party libraries
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/codec"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc/bare"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

func newBlockStore(endpoint string) (*BlockStore, error) {
	s := &BlockStore{
		RefCount: primitive.RefCount{},
		client: bare.New(endpoint, rpc.NewClientFunc(func(conn *grpc.ClientConn) interface{} {
			return segpb.NewSegmentServerClient(conn)
		})),
		tracer: tracing.NewTracer("internal.store.BlockStore", trace.SpanKindClient),
	}
	_, err := s.client.Get(context.Background())
	if err != nil {
		// TODO: check error
		return nil, err
	}
	return s, nil
}

type BlockStore struct {
	primitive.RefCount
	client rpc.Client
	tracer *tracing.Tracer
}

func (s *BlockStore) Endpoint() string {
	return s.client.Endpoint()
}

func (s *BlockStore) Close() {
	s.client.Close()
}

func (s *BlockStore) Append(ctx context.Context, block uint64, event *ce.Event) (int64, error) {
	_ctx, span := s.tracer.Start(ctx, "Append")
	defer span.End()

	eventpb, err := codec.ToProto(event)
	if err != nil {
		return -1, err
	}
	req := &segpb.AppendToBlockRequest{
		BlockId: block,
		Events: &cepb.CloudEventBatch{
			Events: []*cepb.CloudEvent{eventpb},
		},
	}

	client, err := s.client.Get(_ctx)
	if err != nil {
		return -1, err
	}

	res, err := client.(segpb.SegmentServerClient).AppendToBlock(_ctx, req)
	if err != nil {
		if errStatus, ok := status.FromError(err); ok {
			if errType, ok := errpb.Convert(errStatus.Message()); ok {
				if errType.Code == errpb.ErrorCode_SEGMENT_FULL {
					return -1, errors.ErrNoSpace
				}
				return -1, errors.ErrNotWritable
			}
		}
		return -1, err
	}
	// TODO(Y. F. Zhang): batch events
	return res.GetOffsets()[0], nil
}

func (s *BlockStore) Read(
	ctx context.Context, block uint64, offset int64, size int16, pollingTimeout uint32,
) ([]*ce.Event, error) {
	ctx, span := s.tracer.Start(ctx, "Append")
	defer span.End()

	req := &segpb.ReadFromBlockRequest{
		BlockId:        block,
		Offset:         offset,
		Number:         int64(size),
		PollingTimeout: pollingTimeout,
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := client.(segpb.SegmentServerClient).ReadFromBlock(ctx, req)
	if err != nil {
		// TODO: convert error
		if errStatus, ok := status.FromError(err); ok {
			errMsg := errStatus.Message()
			if strings.Contains(errMsg, "the offset on end") {
				err = errors.ErrOnEnd
			} else if strings.Contains(errMsg, "the offset exceeded") {
				err = errors.ErrOverflow
			} else if errStatus.Code() == codes.DeadlineExceeded {
				err = errors.ErrTimeout
			}
		}
		return nil, err
	}

	if batch := resp.GetEvents(); batch != nil {
		if eventpbs := batch.GetEvents(); len(eventpbs) > 0 {
			events := make([]*ce.Event, 0, len(eventpbs))
			for _, eventpb := range eventpbs {
				event, err2 := codec.FromProto(eventpb)
				if err2 != nil {
					// TODO: return events or error?
					return events, err2
				}
				events = append(events, event)
			}
			return events, nil
		}
	}

	return []*ce.Event{}, err
}
