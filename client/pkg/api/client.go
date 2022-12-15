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

//go:generate mockgen -source=client.go  -destination=mock_client.go -package=api
package api

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2"
)

// TODO(jiangkai): currently, only business layer error is returned, because
// the eventlogid cannot be obtained here, so the eventid cannot be generated.
type Callback func(error)

type Eventbus interface {
	Writer(opts ...WriteOption) BusWriter
	Reader(opts ...ReadOption) BusReader

	GetLog(ctx context.Context, logID uint64, opts ...LogOption) (Eventlog, error)
	ListLog(ctx context.Context, opts ...LogOption) ([]Eventlog, error)
	Close(ctx context.Context)
}

type BusWriter interface {
	AppendOne(ctx context.Context, event *ce.Event, opts ...WriteOption) (eid string, err error)
	AppendMany(ctx context.Context, events []*ce.Event, opts ...WriteOption) (eid string, err error)
	AppendOneStream(ctx context.Context, event *ce.Event, cb Callback, opts ...WriteOption)
}

type BusReader interface {
	Read(ctx context.Context, opts ...ReadOption) ([]*ce.Event, int64, uint64, error)
}

type Eventlog interface {
	ID() uint64
	EarliestOffset(ctx context.Context) (int64, error)
	LatestOffset(ctx context.Context) (int64, error)
	Length(ctx context.Context) (int64, error)
	QueryOffsetByTime(ctx context.Context, timestamp int64) (int64, error)
}
