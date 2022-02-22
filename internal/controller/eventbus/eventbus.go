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
	"github.com/golang/protobuf/proto"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)


func initController() {

}

var (
	c  *controller
	once sync.Once
)

// TODO error
func NewEventBusController() ctrl.EventBusControllerServer {
	once.Do(initController)
	return c
}

func NewEventLogController() ctrl.EventLogControllerServer {
	once.Do(initController)
	return c
}

func NewSegmentController() ctrl.SegmentControllerServer {
	once.Do(initController)
	return c
}

type controller struct {
	kvStore kv.Client
}

func (ctrl *controller) CreateEventBus(ctx context.Context, req *ctrl.CreateEventBusRequest) (*meta.EventBus, error) {
	eb := &meta.EventBus{
		Namespace: req.Namespace,
		Name:      req.Name,
		LogNumber: req.LogNumber,
		Logs:      make([]*meta.EventLog, req.LogNumber),
	}
	eb.Vrn = ctrl.generateEventBusVRN(eb)
	exist, err := ctrl.kvStore.Exists(eb.Vrn.Value)
	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "invoke kv exist failed", err)
	}
	if exist {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "eventbus resource name conflicted")
	}
	wg := sync.WaitGroup{}
	for idx := 0; idx < int(eb.LogNumber); idx++ {
		eb.Logs[idx] = &meta.EventLog{
			EventLogId:            int64(idx),
			BusVrn:                eb.Vrn,
			CurrentSegmentNumbers: 0,
		}
		eb.Logs[idx].Vrn = ctrl.generateEventLogVRN(eb.Logs[idx])
		wg.Add(1)
		go func(i int) {
			_err := ctrl.initializeEventLog(eb.Logs[i])
			// TODO thread safety
			err = errors.Chain(err, _err)
			wg.Done()
		}(idx)
	}
	wg.Wait()

	if err != nil {
		return nil, errors.ConvertGRPCError(errors.NotBeenClassified, "initialized eventlog failed", err)
	}

	data, _ := proto.Marshal(eb)
	ctrl.kvStore.Set(eb.Vrn.Value, data)
	return nil, nil
}

func (ctrl *controller) DeleteEventBus(ctx context.Context, vrn *meta.VanusResourceName) (*emptypb.Empty, error) {
	return nil, nil
}

func (ctrl *controller) GetEventBus(ctx context.Context, vrn *meta.VanusResourceName) (*meta.EventBus, error) {
	return nil, nil
}

func (ctrl *controller) UpdateEventBus(ctx context.Context, req *ctrl.UpdateEventBusRequest) (*meta.EventBus, error) {
	return nil, nil
}

func (ctrl *controller) ListSegment(ctx context.Context, vrn *meta.VanusResourceName) (*ctrl.ListSegmentResponse, error) {
	return nil, nil
}

func (ctrl *controller) QuerySegmentRouteInfo(ctx context.Context, req *ctrl.QuerySegmentRouteInfoRequest) (*ctrl.QuerySegmentRouteInfoResponse, error) {
	return nil, nil
}

func (ctrl *controller) SegmentHeartbeat(srv ctrl.SegmentController_SegmentHeartbeatServer) error {

	return nil
}

func (ctrl *controller) initializeEventLog(el *meta.EventLog) error {
	return nil
}

func (ctrl *controller) generateEventBusVRN(eb *meta.EventBus) *meta.VanusResourceName {
	return &meta.VanusResourceName{}
}

func (ctrl *controller) generateEventLogVRN(eb *meta.EventLog) *meta.VanusResourceName {
	return &meta.VanusResourceName{}
}
