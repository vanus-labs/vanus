// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventbus

import (
	"context"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
)

var c = &controller{}

func NewEventBusController() ctrl.EventBusControllerServer {
	return c
}

func NewEventLogController() ctrl.EventLogControllerServer {
	return c
}

func NewSegmentController() ctrl.SegmentControllerServer {
	return c
}

type controller struct {
}

func (ctrl *controller) CreateEventBus(context.Context, *ctrl.CreateEventBusRequest) (*meta.EventBus, error) {
	return nil, nil
}

func (ctrl *controller) DeleteEventBus(context.Context, *ctrl.DeleteEventBusRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (ctrl *controller) GetEventBus(context.Context, *ctrl.GetEventBusRequest) (*meta.EventBus, error) {
	return nil, nil
}

func (ctrl *controller) UpdateEventBus(context.Context, *ctrl.UpdateEventBusRequest) (*meta.EventBus, error) {
	return nil, nil
}

func (ctrl *controller) ListSegment(context.Context, *ctrl.ListSegmentRequest) (*ctrl.ListSegmentResponse, error) {
	return nil, nil
}

func (ctrl *controller) QuerySegmentRouteInfo(context.Context, *ctrl.QuerySegmentRouteInfoRequest) (*ctrl.QuerySegmentRouteInfoResponse, error) {
	return nil, nil
}

func (ctrl *controller) SegmentHeartbeat(srv ctrl.SegmentController_SegmentHeartbeatServer) error {

	return nil
}
