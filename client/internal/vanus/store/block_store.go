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
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"
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

const (
	readMessageFromServerTimeoutInSecond = 5 * time.Second
)

func newBlockStore(endpoint string) (*BlockStore, error) {
	s := &BlockStore{
		RefCount: primitive.RefCount{},
		client: bare.New(endpoint, rpc.NewClientFunc(func(conn *grpc.ClientConn) interface{} {
			return segpb.NewSegmentServerClient(conn)
		})),
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
}

func (s *BlockStore) Endpoint() string {
	return s.client.Endpoint()
}

func (s *BlockStore) Close() {
	s.client.Close()
}

func (s *BlockStore) Append(ctx context.Context, block uint64, event *ce.Event) (int64, error) {
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

	client, err := s.client.Get(ctx)
	if err != nil {
		return -1, err
	}

	_, err = client.(segpb.SegmentServerClient).AppendToBlock(ctx, req)
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
	// FIXME: return offset
	return 0, nil
}

func (s *BlockStore) Read(ctx context.Context, block uint64, offset int64, size int16) ([]*ce.Event, error) {
	req := &segpb.ReadFromBlockRequest{
		BlockId: block,
		Offset:  offset,
		Number:  int64(size),
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	tCtx, cancel := context.WithTimeout(ctx, readMessageFromServerTimeoutInSecond)
	var events []*ce.Event
	reqId, _ := uuid.NewUUID()
	go func() {
		defer cancel()
		println("===========")
		fmt.Printf("req: %s, time: %v\n", reqId.String(), time.Now())
		resp, err := client.(segpb.SegmentServerClient).ReadFromBlock(tCtx, req)
		if err != nil {
			// TODO: convert error
			if errStatus, ok := status.FromError(err); ok {
				errMsg := errStatus.Message()
				if strings.Contains(errMsg, "the offset on end") {
					err = errors.ErrOnEnd
				} else if strings.Contains(errMsg, "the offset exceeded") {
					err = errors.ErrOverflow
				}
			}
			return
		}

		if batch := resp.GetEvents(); batch != nil {
			if eventpbs := batch.GetEvents(); len(eventpbs) > 0 {
				for _, eventpb := range eventpbs {
					event, err := codec.FromProto(eventpb)
					if err != nil {
						// TODO: return events or error?
						return
					}
					events = append(events, event)
				}
			}
		}
	}()
	select {
	case <-ctx.Done():
		cancel()
		return nil, errors.ErrTimeout
	case <-tCtx.Done():
		if tCtx.Err() == context.DeadlineExceeded {
			return nil, errors.ErrTimeout
		}
		fmt.Printf("req: %s, time: %v\n", reqId.String(), time.Now())
	}

	return events, err
}
