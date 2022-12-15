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
	"math/rand"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries
	cepb "cloudevents.io/genproto/v1"
	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"

	// first-party libraries
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project
	"github.com/linkall-labs/vanus/client/internal/vanus/codec"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc"
	"github.com/linkall-labs/vanus/client/internal/vanus/net/rpc/bare"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
	"github.com/linkall-labs/vanus/observability/log"
)

func newBlockStore(endpoint string) (*BlockStore, error) {
	var err error
	s := &BlockStore{
		RefCount: primitive.RefCount{},
		client: bare.New(endpoint, rpc.NewClientFunc(func(conn *grpc.ClientConn) interface{} {
			return segpb.NewSegmentServerClient(conn)
		})),
		tracer: tracing.NewTracer("internal.store.BlockStore", trace.SpanKindClient),
	}
	s.appendStream, err = s.getAppendStream(context.Background())
	if err != nil {
		// TODO: check error
		return nil, err
	}
	s.readStream, err = s.getReadStream(context.Background())
	if err != nil {
		// TODO: check error
		return nil, err
	}
	s.receive(context.Background(), s.appendStream)
	return s, nil
}

type BlockStore struct {
	primitive.RefCount
	client       rpc.Client
	tracer       *tracing.Tracer
	appendStream segpb.SegmentServer_AppendToBlockStreamClient
	readStream   segpb.SegmentServer_ReadFromBlockStreamClient
	appendMu     sync.Mutex
	readMu       sync.Mutex
	callbacks    sync.Map
}

func (s *BlockStore) receive(ctx context.Context, stream segpb.SegmentServer_AppendToBlockStreamClient) {
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				log.Debug(context.Background(), "append stream receive failed", map[string]interface{}{
					log.KeyError: err,
					"endpoint":   s.Endpoint(),
				})
				break
			}
			c, _ := s.callbacks.LoadAndDelete(res.ResponseId)
			if c != nil {
				c.(api.Callback)(err)
			}
		}
	}()
}

func (s *BlockStore) getAppendStream(ctx context.Context) (segpb.SegmentServer_AppendToBlockStreamClient, error) {
	if s.appendStream != nil {
		return s.appendStream, nil
	}

	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	if s.appendStream != nil { //double check
		return s.appendStream, nil
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.(segpb.SegmentServerClient).AppendToBlockStream(context.Background())
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *BlockStore) getReadStream(ctx context.Context) (segpb.SegmentServer_ReadFromBlockStreamClient, error) {
	if s.readStream != nil {
		return s.readStream, nil
	}

	s.readMu.Lock()
	defer s.readMu.Unlock()

	if s.readStream != nil { //double check
		return s.readStream, nil
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.(segpb.SegmentServerClient).ReadFromBlockStream(context.Background())
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *BlockStore) closeAppendStream() {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	s.appendStream.CloseSend()
	s.appendStream = nil
}

func (s *BlockStore) closeReadStream() {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	s.readStream.CloseSend()
	s.readStream = nil
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
		return -1, err
	}
	return res.GetOffsets()[0], nil
}

func (s *BlockStore) AppendStream(ctx context.Context, block uint64, event *ce.Event, cb api.Callback) {
	_ctx, span := s.tracer.Start(ctx, "AppendStream")
	defer span.End()

	var (
		err error
	)

	if s.appendStream == nil {
		s.appendStream, err = s.getAppendStream(_ctx)
		if err != nil {
			cb(err)
			return
		}
		s.receive(ctx, s.appendStream)
	}

	// generate unique RequestId
	requestID := rand.Uint64()
	s.callbacks.Store(requestID, cb)

	retFunc := func(err error) {
		c, _ := s.callbacks.LoadAndDelete(requestID)
		if c != nil {
			c.(api.Callback)(err)
		}
	}
	eventpb, err := codec.ToProto(event)
	if err != nil {
		retFunc(err)
		return
	}
	req := &segpb.AppendToBlockStreamRequest{
		RequestId: requestID,
		BlockId:   block,
		Events: &cepb.CloudEventBatch{
			Events: []*cepb.CloudEvent{eventpb},
		},
	}
	if err = s.appendStream.Send(req); err != nil {
		log.Debug(context.Background(), "append stream send failed", map[string]interface{}{
			log.KeyError: err,
			"endpoint":   s.Endpoint(),
		})
		s.closeAppendStream()
		retFunc(err)
		return
	}
}

func (s *BlockStore) Read(
	ctx context.Context, block uint64, offset int64, size int16, pollingTimeout uint32,
) ([]*ce.Event, error) {
	ctx, span := s.tracer.Start(ctx, "Read")
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

func (s *BlockStore) ReadStream(
	ctx context.Context, block uint64, offset int64, size int16, pollingTimeout uint32,
) ([]*ce.Event, error) {
	ctx, span := s.tracer.Start(ctx, "ReadStream")
	defer span.End()

	var (
		err error
	)

	if s.readStream == nil {
		s.readStream, err = s.getReadStream(ctx)
		if err != nil {
			return nil, err
		}
	}

	// generate unique RequestId
	requestID := rand.Uint64()
	req := &segpb.ReadFromBlockStreamRequest{
		RequestId:      requestID,
		BlockId:        block,
		Offset:         offset,
		Number:         int64(size),
		PollingTimeout: pollingTimeout,
	}

	if err = s.readStream.Send(req); err != nil {
		log.Debug(context.Background(), "read stream send failed", map[string]interface{}{
			log.KeyError: err,
			"endpoint":   s.Endpoint(),
		})
		s.closeReadStream()
		return nil, err
	}

	resp, err := s.readStream.Recv()
	if err != nil {
		log.Debug(context.Background(), "read stream receive failed", map[string]interface{}{
			log.KeyError: err,
			"endpoint":   s.Endpoint(),
		})
		s.closeReadStream()
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

func (s *BlockStore) LookupOffset(ctx context.Context, blockID uint64, t time.Time) (int64, error) {
	ctx, span := s.tracer.Start(ctx, "LookupOffset")
	defer span.End()

	req := &segpb.LookupOffsetInBlockRequest{
		BlockId: blockID,
		Stime:   t.UnixMilli(),
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return -1, err
	}

	res, err := client.(segpb.SegmentServerClient).LookupOffsetInBlock(ctx, req)
	if err != nil {
		return -1, err
	}
	return res.Offset, nil
}
