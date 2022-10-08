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

//go:generate mockgen -source=eventbus.go  -destination=mock_eventbus.go -package=eventbus
package eventbus

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
)

const (
	XVanusLogOffset = eventlog.XVanusLogOffset
)

type Eventbus interface {
	Writer(opts ...WriteOption) BusWriter
	Reader(opts ...ReadOption) BusReader

	GetLog(ctx context.Context, logID uint64, opts ...LogOption) (eventlog.Eventlog, error)
	ListLog(ctx context.Context, opts ...LogOption) ([]eventlog.Eventlog, error)
}

type BusWriter interface {
	AppendOne(ctx context.Context, event *ce.Event, opts ...WriteOption) (eid string, err error)
	AppendMany(ctx context.Context, events []*ce.Event, opts ...WriteOption) (eid string, err error)
}

type BusReader interface {
	Read(ctx context.Context, size int16, opts ...ReadOption) ([]*ce.Event, int64, uint64, error)
}
