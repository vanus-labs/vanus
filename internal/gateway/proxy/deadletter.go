// Copyright 2023 Linkall Inc.
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

package proxy

import (
	"context"
	"encoding/binary"
	"fmt"

	v2 "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventlog"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"

	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

func (cp *ControllerProxy) GetDeadLetterEvent(
	ctx context.Context, req *proxypb.GetDeadLetterEventRequest,
) (*proxypb.GetDeadLetterEventResponse, error) {
	if req.GetSubscriptionId() == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("subscription is empty")
	}

	subscription, err := cp.triggerCtrl.GetSubscription(ctx,
		&ctrlpb.GetSubscriptionRequest{Id: req.GetSubscriptionId()})
	if err != nil {
		return nil, err
	}
	storeOffset, err := cp.triggerCtrl.GetDeadLetterEventOffset(ctx,
		&ctrlpb.GetDeadLetterEventOffsetRequest{SubscriptionId: req.SubscriptionId})
	if err != nil {
		return nil, err
	}
	var (
		offset = req.Offset
		num    = req.Number
	)

	if offset == 0 {
		offset = storeOffset.GetOffset()
	} else if offset < storeOffset.GetOffset() {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("offset is invalid, param is %d it but now is %d", offset, storeOffset.Offset))
	}
	deadLetterEventbusID, err := cp.getDealLetterEventbusID(ctx, vanus.NewIDFromUint64(subscription.EventbusId))
	if err != nil {
		return nil, err
	}
	ls, err := cp.client.Eventbus(ctx, api.WithID(deadLetterEventbusID.Uint64())).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	earliestOffset, err := ls[0].EarliestOffset(ctx)
	if err != nil {
		return nil, err
	}
	if earliestOffset > 0 && offset < uint64(earliestOffset) {
		offset = uint64(earliestOffset)
	}

	if num > maximumNumberPerGetRequest {
		num = maximumNumberPerGetRequest
	}

	readPolicy := policy.NewManuallyReadPolicy(ls[0], int64(offset))
	busReader := cp.client.Eventbus(ctx, api.WithID(deadLetterEventbusID.Uint64())).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(readPolicy),
		option.WithBatchSize(int(num)),
	)
	subscriptionIDStr := vanus.NewIDFromUint64(req.SubscriptionId).String()
	var events []*v2.Event
loop:
	for {
		_events, _, _, err := api.Read(ctx, busReader)
		if err != nil {
			if errors.Is(err, errors.ErrOffsetOnEnd) {
				// read end
				break
			}
			// todo some error need retry read
			return nil, err
		}
		if len(_events) == 0 {
			break
		}
		for _, v := range _events {
			ec, _ := v.Context.(*v2.EventContextV1)
			if ec.Extensions[primitive.XVanusSubscriptionID] != subscriptionIDStr {
				continue
			}
			events = append(events, v)
			if len(events) == int(num) {
				break loop
			}
		}
		readPolicy.Forward(len(_events))
	}
	results := make([]*wrapperspb.BytesValue, len(events))
	for idx, v := range events {
		data, _ := v.MarshalJSON()
		results[idx] = wrapperspb.Bytes(data)
	}
	return &proxypb.GetDeadLetterEventResponse{
		Events: results,
	}, nil
}

func (cp *ControllerProxy) getDealLetterEventbusID(
	ctx context.Context, eventbusID vanus.ID,
) (vanus.ID, error) {
	deadLetterEventbusName := primitive.GetDeadLetterEventbusName(eventbusID)
	eb, err := cp.ctrl.EventbusService().GetSystemEventbusByName(ctx, deadLetterEventbusName)
	if err != nil {
		return 0, err
	}
	return vanus.NewIDFromUint64(eb.Id), nil
}

func (cp *ControllerProxy) ResendDeadLetterEvent(
	ctx context.Context, req *proxypb.ResendDeadLetterEventRequest,
) (*emptypb.Empty, error) {
	if req.GetSubscriptionId() == 0 {
		return nil, errors.ErrInvalidRequest.WithMessage("subscription is empty")
	}
	subscription, err := cp.triggerCtrl.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{
		Id: req.GetSubscriptionId(),
	})
	if err != nil {
		return nil, err
	}
	subscriptionIDStr := vanus.NewIDFromUint64(req.SubscriptionId).String()
	storeOffset, err := cp.triggerCtrl.GetDeadLetterEventOffset(ctx,
		&ctrlpb.GetDeadLetterEventOffsetRequest{SubscriptionId: req.GetSubscriptionId()})
	if err != nil {
		return nil, err
	}
	offset := req.GetStartOffset()
	if offset == 0 {
		offset = storeOffset.GetOffset()
	} else if offset < storeOffset.GetOffset() {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("start_offset is invalid, param is %d it but now is %d", offset, storeOffset.Offset))
	}
	deadLetterEventbusID, err := cp.getDealLetterEventbusID(ctx, vanus.NewIDFromUint64(subscription.EventbusId))
	if err != nil {
		return nil, err
	}
	ls, err := cp.client.Eventbus(ctx, api.WithID(deadLetterEventbusID.Uint64())).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	earliestOffset, err := ls[0].EarliestOffset(ctx)
	if err != nil {
		return nil, err
	}
	if earliestOffset > 0 && offset < uint64(earliestOffset) {
		offset = uint64(earliestOffset)
	}
	if req.GetEndOffset() != 0 && req.GetEndOffset() < offset {
		return nil, errors.ErrInvalidRequest.WithMessage(
			fmt.Sprintf("end_offset is invalid, param is %d it but start is %d", offset, req.GetEndOffset()))
	}
	readPolicy := policy.NewManuallyReadPolicy(ls[0], int64(offset))
	busReader := cp.client.Eventbus(ctx, api.WithID(deadLetterEventbusID.Uint64())).Reader(
		option.WithDisablePolling(),
		option.WithReadPolicy(readPolicy),
		option.WithBatchSize(readSize),
	)
	var endOffset uint64
	var events []*cloudevents.CloudEvent
loop:
	for {
		_events, _, _, err2 := api.Read(ctx, busReader)
		if err2 != nil {
			if errors.Is(err2, errors.ErrOffsetOnEnd) { // read end
				break
			}
			// todo errors.ErrTryAgain maybe need retry read
			return nil, err2
		}
		if len(_events) == 0 {
			break
		}
		for _, v := range _events {
			ec, _ := v.Context.(*v2.EventContextV1)
			offsetByte, _ := ec.Extensions[eventlog.XVanusLogOffset].([]byte)
			_endOffset := binary.BigEndian.Uint64(offsetByte)
			if req.GetEndOffset() != 0 && _endOffset > req.GetEndOffset() {
				break loop
			}
			endOffset = _endOffset
			if ec.Extensions[primitive.XVanusSubscriptionID] != subscriptionIDStr {
				continue
			}
			// remove retry attribute
			delete(ec.Extensions, primitive.XVanusRetryAttempts)
			// remove dead letter attribute
			delete(ec.Extensions, primitive.LastDeliveryTime)
			delete(ec.Extensions, primitive.LastDeliveryError)
			delete(ec.Extensions, primitive.DeadLetterReason)
			pbEvent, err3 := codec.ToProto(v)
			if err3 != nil {
				return nil, err3
			}
			events = append(events, pbEvent)
		}
		readPolicy.Forward(len(_events))
		if len(events) > 10 {
			err2 = cp.writeDeadLetterEvent(ctx, req.SubscriptionId, endOffset+1, events)
			if err2 != nil {
				return nil, err2
			}
			events = nil
		}
	}
	if len(events) != 0 {
		err = cp.writeDeadLetterEvent(ctx, req.SubscriptionId, endOffset+1, events)
		if err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (cp *ControllerProxy) writeDeadLetterEvent(
	ctx context.Context, subscriptionID uint64, offset uint64, events []*cloudevents.CloudEvent,
) error {
	// write to retry eventbus
	err := cp.writeEvents(ctx, primitive.RetryEventbusName, &cloudevents.CloudEventBatch{
		Events: events,
	})
	if err != nil {
		return errors.ErrInternal.Wrap(err).WithMessage("write event error")
	}
	// save offset
	_, err = cp.triggerCtrl.SetDeadLetterEventOffset(ctx, &ctrlpb.SetDeadLetterEventOffsetRequest{
		SubscriptionId: subscriptionID, Offset: offset,
	})
	if err != nil {
		return errors.ErrInternal.Wrap(err).WithMessage("save offset error")
	}
	return nil
}
