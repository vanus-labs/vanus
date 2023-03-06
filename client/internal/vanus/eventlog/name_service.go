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
	"time"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/tracing"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"

	// this project.
	"github.com/vanus-labs/vanus/client/pkg/record"
)

func NewNameService(endpoints []string) *NameService {
	return &NameService{
		client: cluster.NewClusterController(endpoints, insecure.NewCredentials()).EventlogService().RawClient(),
		tracer: tracing.NewTracer("internal.discovery.eventlog", trace.SpanKindClient),
	}
}

type NameService struct {
	client ctrlpb.EventLogControllerClient
	tracer *tracing.Tracer
}

func (ns *NameService) LookupWritableSegment(ctx context.Context, logID uint64) (*record.Segment, error) {
	ctx, span := ns.tracer.Start(ctx, "LookupWritableSegment")
	defer span.End()

	req := &ctrlpb.GetAppendableSegmentRequest{
		EventLogId: logID,
		Limited:    1,
	}

	resp, err := ns.client.GetAppendableSegment(ctx, req)
	if err != nil {
		log.Error(context.Background(), "get appendable segment failed", map[string]interface{}{
			log.KeyError: err,
			"eventlog":   logID,
			"resp":       resp.String(),
		})
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

	req := &ctrlpb.ListSegmentRequest{
		EventLogId:  logID,
		StartOffset: 0,
		EndOffset:   math.MaxInt64,
		Limited:     math.MaxInt32,
	}

	resp, err := ns.client.ListSegment(ctx, req)
	if err != nil {
		log.Error(context.Background(), "list segment failed", map[string]interface{}{
			log.KeyError: err,
			"eventlog":   logID,
			"resp":       resp.String(),
		})
		return nil, err
	}

	segments := toSegments(resp.GetSegments())
	return segments, nil
}

func toSegments(pbs []*metapb.Segment) []*record.Segment {
	if len(pbs) == 0 {
		return make([]*record.Segment, 0)
	}
	segments := make([]*record.Segment, 0, len(pbs))
	for _, pb := range pbs {
		segment := toSegment(pb)
		segments = append(segments, segment)
		// only return first working segment
		if segment.Writable {
			break
		}
	}
	return segments
}

func toSegment(segment *metapb.Segment) *record.Segment {
	blocks := make(map[uint64]*record.Block, len(segment.Replicas))
	for blockID, block := range segment.Replicas {
		blocks[blockID] = &record.Block{
			ID:       block.Id,
			Endpoint: block.Endpoint,
		}
	}
	return &record.Segment{
		ID:               segment.GetId(),
		StartOffset:      segment.GetStartOffsetInLog(),
		EndOffset:        segment.GetEndOffsetInLog(),
		FirstEventBornAt: time.UnixMilli(segment.FirstEventBornAtByUnixMs),
		LastEventBornAt:  time.UnixMilli(segment.LastEvnetBornAtByUnixMs),
		Writable:         segment.State == "working", // TODO: writable
		Blocks:           blocks,
		LeaderBlockID:    segment.GetLeaderBlockId(),
	}
}
