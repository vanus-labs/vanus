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
	stderr "errors"
	"io"
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

const (
	defaultTaskChannelBuffer = 512
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
	s.stream, err = s.connect(context.Background())
	if err != nil {
		// TODO: check error
		return nil, err
	}
	s.taskC = make(chan Task, defaultTaskChannelBuffer)
	s.run(context.Background())
	s.receive(context.Background(), s.stream)
	return s, nil
}

type BlockStore struct {
	primitive.RefCount
	client    rpc.Client
	tracer    *tracing.Tracer
	stream    segpb.SegmentServer_AppendToBlockStreamClient
	taskC     chan Task
	callbacks sync.Map
	mu        sync.Mutex
}

type Task struct {
	request *segpb.AppendToBlockStreamRequest
	cb      api.Callback
}

func (s *BlockStore) run(ctx context.Context) {
	go func() {
		for {
			var err error
			select {
			case <-ctx.Done():
				s.stream.CloseSend()
				s.stream = nil
				return
			case task := <-s.taskC:
				stream := s.stream
				if stream == nil {
					if stream, err = s.connect(ctx); err != nil {
						task.cb(err)
						break
					}
					s.receive(ctx, stream)
					s.stream = stream
				}
				if err = stream.Send(task.request); err != nil {
					log.Warning(ctx, "===Send failed===", map[string]interface{}{
						log.KeyError: err,
					})
					s.processSendError(task, err)
				}
			}
		}
	}()
}

func (s *BlockStore) receive(ctx context.Context, stream segpb.SegmentServer_AppendToBlockStreamClient) {
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				log.Warning(ctx, "===Recv failed===", map[string]interface{}{
					log.KeyError: err,
				})
				break
			}
			c, _ := s.callbacks.LoadAndDelete(res.ResponseId)
			if c == nil {
				// TODO(jiangkai): check err
				continue
			}
			if res.ResponseCode != segpb.ResponseCode_SUCCESS {
				c.(api.Callback)(stderr.New(res.ResponseCode.String()))
			}
		}
	}()
}

func (s *BlockStore) connect(ctx context.Context) (segpb.SegmentServer_AppendToBlockStreamClient, error) {
	if s.stream != nil {
		return s.stream, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream != nil { //double check
		return s.stream, nil
	}

	client, err := s.client.Get(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := client.(segpb.SegmentServerClient).AppendToBlockStream(context.Background())
	if err != nil {
		log.Warning(ctx, "===Get Stream failed===", map[string]interface{}{
			log.KeyError: err,
		})
		return nil, err
	}
	return stream, nil
}

func (s *BlockStore) processSendError(t Task, err error) {
	cb, _ := s.callbacks.LoadAndDelete(t.request.RequestId)
	if cb != nil {
		cb.(api.Callback)(err)
	}
	if stderr.Is(err, io.EOF) {
		s.stream.CloseSend()
		s.stream = nil
	}
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
	_, span := s.tracer.Start(ctx, "AppendStream")
	defer span.End()

	var (
		err error
	)

	eventpb, err := codec.ToProto(event)
	if err != nil {
		cb(err)
		return
	}

	// generate unique RequestId
	requestID := rand.Uint64()
	s.callbacks.Store(requestID, cb)
	task := Task{
		request: &segpb.AppendToBlockStreamRequest{
			RequestId: requestID,
			BlockId:   block,
			Events: &cepb.CloudEventBatch{
				Events: []*cepb.CloudEvent{eventpb},
			},
		},
		cb: cb,
	}
	s.taskC <- task
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
