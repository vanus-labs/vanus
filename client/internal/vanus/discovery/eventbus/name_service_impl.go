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
	// standard libraries
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"sort"
	"strings"

	// third-party libraries
	"github.com/linkall-labs/vanus/pkg/controller"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
)

func newNameServiceImpl(endpoints []string) (*nameServiceImpl, error) {
	ns := &nameServiceImpl{
		client: controller.NewEventbusClient(endpoints, insecure.NewCredentials()),
		tracer: tracing.NewTracer("internal.discovery.eventbus", trace.SpanKindClient),
	}

	return ns, nil
}

type nameServiceImpl struct {
	//client       rpc.Client
	client ctrlpb.EventBusControllerClient
	tracer *tracing.Tracer
}

// make sure nameServiceImpl implements discovery.NameService.
var _ discovery.NameService = (*nameServiceImpl)(nil)

func (ns *nameServiceImpl) LookupWritableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	// TODO: use list
	// req := &ctlpb.ListEventLogsRequest{
	// 	Parent:       eventbus,
	// 	WritableOnly: true,
	// }
	// resp, err := ns.client.ListEventLogs(context.Background(), req)
	ctx, span := ns.tracer.Start(ctx, "LookupWritableLogs")
	defer span.End()

	req := &metapb.EventBus{
		Id:   eventbus.ID,
		Name: eventbus.Name,
	}

	resp, err := ns.client.GetEventBus(ctx, req)

	if err != nil {
		return nil, err
	}
	return toLogs(resp.GetLogs()), nil
}

func (ns *nameServiceImpl) LookupReadableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	// TODO: use list
	// req := &ctlpb.ListEventLogsRequest{
	// 	Parent:       eventbus,
	// 	ReadableOnly: true,
	// }
	// resp, err := ns.client.ListEventLogs(context.Background(), req)
	ctx, span := ns.tracer.Start(ctx, "LookupReadableLogs")
	defer span.End()

	req := &metapb.EventBus{
		Id:   eventbus.ID,
		Name: eventbus.Name,
	}

	resp, err := ns.client.GetEventBus(ctx, req)
	if err != nil {
		return nil, err
	}

	return toLogs(resp.GetLogs()), nil
}

func toLogs(logpbs []*metapb.EventLog) []*record.EventLog {
	if len(logpbs) <= 0 {
		return make([]*record.EventLog, 0, 0)
	}
	logs := make([]*record.EventLog, 0, len(logpbs))
	for _, logpb := range logpbs {
		logs = append(logs, toLog(logpb))
	}
	return logs
}

func toLog(logpb *metapb.EventLog) *record.EventLog {
	addrs := logpb.GetServerAddress()
	if len(addrs) <= 0 {
		// FIXME: missing address
		addrs = []string{"localhost:2048"}
	}
	sort.Strings(addrs)
	controllers := strings.Join(addrs, ",")
	log := &record.EventLog{
		// TODO: format of vrn
		VRN:  fmt.Sprintf("vanus:///eventlog/%d?eventbus=%s&controllers=%s", logpb.GetEventLogId(), logpb.GetEventBusName(), controllers),
		Mode: record.PremWrite | record.PremRead,
		// Mode: record.LogMode(logpb.GetMode()),
	}
	return log
}
