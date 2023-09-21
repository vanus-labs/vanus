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

package eventbus

import (
	// standard libraries.
	"context"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	"github.com/vanus-labs/vanus/api/cluster"
	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	metapb "github.com/vanus-labs/vanus/api/meta"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/tracing"

	// this project.
	"github.com/vanus-labs/vanus/client/pkg/record"
)

func NewNameService(endpoints []string) *NameService {
	return &NameService{
		client: cluster.NewClusterController(endpoints, insecure.NewCredentials()).EventbusService().RawClient(),
		tracer: tracing.NewTracer("internal.discovery.eventbus", trace.SpanKindClient),
	}
}

type NameService struct {
	client ctrlpb.EventbusControllerClient
	tracer *tracing.Tracer
}

func (ns *NameService) LookupWritableLogs(ctx context.Context, eventbusID uint64) ([]*record.Eventlog, error) {
	ctx, span := ns.tracer.Start(ctx, "LookupWritableLogs")
	defer span.End()

	req := &wrapperspb.UInt64Value{
		Value: eventbusID,
	}

	resp, err := ns.client.GetEventbus(ctx, req)
	if err != nil {
		log.Debug().Err(err).Uint64("eventbus_id", eventbusID).Msg("get eventbus failed")
		return nil, err
	}
	return toLogs(resp.GetLogs()), nil
}

func (ns *NameService) LookupReadableLogs(ctx context.Context, eventbusID uint64) ([]*record.Eventlog, error) {
	ctx, span := ns.tracer.Start(ctx, "LookupReadableLogs")
	defer span.End()

	req := &wrapperspb.UInt64Value{
		Value: eventbusID,
	}

	resp, err := ns.client.GetEventbus(ctx, req)
	if err != nil {
		log.Debug().Err(err).Uint64("eventbus_id", eventbusID).Msg("get eventbus failed")
		return nil, err
	}

	return toLogs(resp.GetLogs()), nil
}

func toLogs(logpbs []*metapb.Eventlog) []*record.Eventlog {
	if len(logpbs) <= 0 {
		return make([]*record.Eventlog, 0)
	}
	logs := make([]*record.Eventlog, 0, len(logpbs))
	for _, logpb := range logpbs {
		logs = append(logs, toLog(logpb))
	}
	return logs
}

func toLog(logpb *metapb.Eventlog) *record.Eventlog {
	log := &record.Eventlog{
		ID:   logpb.GetEventlogId(),
		Mode: record.PremWrite | record.PremRead,
	}
	return log
}
