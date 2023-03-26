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

//go:generate mockgen -source=client.go -destination=mock_client.go -package=api
package api

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
)

type CloseFunc func(id uint64)

type Client interface {
	Eventbus(ctx context.Context, opts ...EventbusOption) (Eventbus, error)
	Disconnect(ctx context.Context)
}

type Eventbus interface {
	Writer(opts ...WriteOption) BusWriter
	Reader(opts ...ReadOption) BusReader

	GetLog(ctx context.Context, logID uint64, opts ...LogOption) (Eventlog, error)
	ListLog(ctx context.Context, opts ...LogOption) ([]Eventlog, error)
	Close(ctx context.Context)
}

type BusWriter interface {
	Append(ctx context.Context, events *cloudevents.CloudEventBatch, opts ...WriteOption) (eids []string, err error)
}

type BusReader interface {
	Read(ctx context.Context, opts ...ReadOption) (events *cloudevents.CloudEventBatch, off int64, logid uint64, err error)
}

type Eventlog interface {
	ID() uint64
	EarliestOffset(ctx context.Context) (int64, error)
	LatestOffset(ctx context.Context) (int64, error)
	Length(ctx context.Context) (int64, error)
	QueryOffsetByTime(ctx context.Context, timestamp int64) (int64, error)
}

func Append(ctx context.Context, w BusWriter, events []*ce.Event, opts ...WriteOption) (eids []string, err error) {
	eventpbs := make([]*cloudevents.CloudEvent, len(events))
	for idx := range events {
		eventpb, err := codec.ToProto(events[idx])
		if err != nil {
			return nil, err
		}
		eventpbs[idx] = eventpb
	}
	return w.Append(ctx, &cloudevents.CloudEventBatch{
		Events: eventpbs,
	}, opts...)
}

func AppendOne(ctx context.Context, w BusWriter, event *ce.Event, opts ...WriteOption) (eid string, err error) {
	eventpb, err := codec.ToProto(event)
	if err != nil {
		return "", err
	}
	eids, err := w.Append(ctx, &cloudevents.CloudEventBatch{
		Events: []*cloudevents.CloudEvent{eventpb},
	}, opts...)
	if err != nil {
		return "", err
	}
	return eids[0], nil
}

func Read(ctx context.Context, r BusReader, opts ...ReadOption) (events []*ce.Event, off int64, logid uint64, err error) {
	batch, off, logid, err := r.Read(ctx, opts...)
	if err != nil {
		return nil, off, logid, err
	}
	es := make([]*ce.Event, len(batch.Events))
	for idx := range batch.Events {
		e, err := codec.FromProto(batch.Events[idx])
		if err != nil {
			return nil, 0, 0, err
		}
		es[idx] = e
	}
	return es, off, logid, nil
}
