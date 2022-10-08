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

package eventlog

import (
	// standard libraries.
	"context"
	"math"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/linkall-labs/vanus/pkg/controller"
	ctlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/record"
)

func NewNameService(endpoints []string) *NameService {
	// TODO: non-blocking now
	// if _, err := ns.Client(); err != nil {
	// 	return nil, err
	// }
	return &NameService{
		client: controller.NewEventlogClient(endpoints, insecure.NewCredentials()),
		tracer: tracing.NewTracer("internal.discovery.eventlog", trace.SpanKindClient),
	}
}

type NameService struct {
	// client       rpc.Client
	client ctlpb.EventLogControllerClient
	tracer *tracing.Tracer
}

func (ns *NameService) LookupWritableSegment(ctx context.Context, logID uint64) (*record.Segment, error) {
	ctx, span := ns.tracer.Start(ctx, "LookupWritableSegment")
	defer span.End()

	// TODO: use standby segments
	req := &ctlpb.GetAppendableSegmentRequest{
		EventLogId: logID,
		Limited:    1,
	}

	resp, err := ns.client.GetAppendableSegment(ctx, req)
	if err != nil {
		return nil, err
	}

	segments := toSegments(resp.GetSegments())
	if len(segments) == 0 {
		return nil, errors.ErrNotWritable
	}
	return segments[0], nil
}

func (ns *NameService) LookupReadableSegments(ctx context.Context, logID uint64) ([]*record.Segment, error) {
	ctx, span := ns.tracer.Start(ctx, "LookupReadableSegments")
	defer span.End()

	// TODO: use range
	req := &ctlpb.ListSegmentRequest{
		EventLogId:  logID,
		StartOffset: 0,
		EndOffset:   math.MaxInt64,
		Limited:     math.MaxInt32,
	}

	resp, err := ns.client.ListSegment(ctx, req)
	if err != nil {
		return nil, err
	}

	segments := toSegments(resp.GetSegments())
	return segments, nil
}

func toSegments(segmentpbs []*metapb.Segment) []*record.Segment {
	if len(segmentpbs) == 0 {
		return make([]*record.Segment, 0)
	}
	segments := make([]*record.Segment, 0, len(segmentpbs))
	for _, segmentpb := range segmentpbs {
		segment := toSegment(segmentpb)
		segments = append(segments, segment)
		// only return first working segment
		if segment.Writable {
			break
		}
	}
	return segments
}

func toSegment(segmentpb *metapb.Segment) *record.Segment {
	blocks := make(map[uint64]*record.Block, len(segmentpb.Replicas))
	for blockID, blockpb := range segmentpb.Replicas {
		blocks[blockID] = &record.Block{
			ID:       blockpb.Id,
			Endpoint: blockpb.Endpoint,
		}
	}
	segment := &record.Segment{
		ID:          segmentpb.GetId(),
		StartOffset: segmentpb.GetStartOffsetInLog(),
		// TODO align to server side
		EndOffset: segmentpb.GetEndOffsetInLog() + 1,
		// TODO: writable
		Writable:      segmentpb.State == "working",
		Blocks:        blocks,
		LeaderBlockID: segmentpb.GetLeaderBlockId(),
	}
	return segment
}
