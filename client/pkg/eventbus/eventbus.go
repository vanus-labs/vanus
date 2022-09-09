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
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

// For write primitive, there is only EventBus and no Producer.
type EventBus interface {
	primitive.RefCounter

	VRN() *discovery.VRN

	Close(ctx context.Context)
	Writer() (BusWriter, error)
}

type BusWriter interface {
	Bus() EventBus

	Close(ctx context.Context)

	Append(ctx context.Context, event *ce.Event) (eid string, err error)

	WithPicker(picker WriterPicker) BusWriter
}
