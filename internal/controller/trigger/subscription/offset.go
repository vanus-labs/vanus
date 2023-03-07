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

package subscription

import (
	"context"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/errors"

	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

func (m *manager) SaveOffset(ctx context.Context, id vanus.ID, offsets info.ListOffsetInfo, commit bool) error {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return nil
	}
	return m.offsetManager.Offset(ctx, id, offsets, commit)
}

func (m *manager) ResetOffsetByTimestamp(ctx context.Context, id vanus.ID,
	timestamp uint64,
) (info.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return nil, errors.ErrResourceNotFound
	}
	offsets, err := m.getOffsetFromCli(ctx, subscription.Eventbus, primitive.SubscriptionConfig{
		OffsetTimestamp: &timestamp,
		OffsetType:      primitive.Timestamp,
	})
	if err != nil {
		return nil, err
	}
	err = m.offsetManager.Offset(ctx, id, offsets, true)
	if err != nil {
		return nil, err
	}
	log.Info(ctx, "reset offset by timestamp", map[string]interface{}{
		"offsets": offsets,
	})
	return offsets, err
}

func (m *manager) GetOffset(ctx context.Context, id vanus.ID) (info.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return info.ListOffsetInfo{}, errors.ErrResourceNotFound
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return nil, err
	}
	// todo filter retry and deadLetter eventlog
	return offsets, nil
}

func (m *manager) GetOrSaveOffset(ctx context.Context, id vanus.ID) (info.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return info.ListOffsetInfo{}, errors.ErrResourceNotFound
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(offsets) > 0 {
		return offsets, nil
	}

	offsets, err = m.getOffsetFromCli(ctx, subscription.Eventbus, subscription.Config)
	if err != nil {
		return nil, err
	}
	// get retry eb offset.
	retryOffset, err := m.getOffsetFromCli(ctx,
		primitive.GetRetryEventbusName(subscription.Eventbus),
		primitive.SubscriptionConfig{
			OffsetType: primitive.LatestOffset,
		})
	if err != nil {
		return nil, err
	}
	offsets = append(offsets, retryOffset...)
	err = m.offsetManager.Offset(ctx, id, offsets, true)
	if err != nil {
		return nil, err
	}
	log.Info(ctx, "save offset from cli", map[string]interface{}{
		"offsets": offsets,
	})
	return offsets, nil
}

func (m *manager) GetDeadLetterOffset(ctx context.Context, id vanus.ID) (uint64, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return 0, errors.ErrResourceNotFound
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return 0, err
	}
	if m.deadLetterEventlogID == 0 {
		logIDs, err := m.getEventlogIDFromCli(ctx,
			primitive.GetDeadLetterEventbusName(subscription.Eventbus))
		if err != nil {
			return 0, err
		}
		m.deadLetterEventlogID = logIDs[0]
	}
	for _, offset := range offsets {
		if offset.EventlogID == m.deadLetterEventlogID {
			return offset.Offset, err
		}
	}
	// storage offsets no exist
	t := uint64(subscription.CreatedAt.Unix())
	cliOffsets, err := m.getOffsetFromCli(ctx,
		primitive.GetDeadLetterEventbusName(subscription.Eventbus),
		primitive.SubscriptionConfig{
			OffsetTimestamp: &t,
			OffsetType:      primitive.Timestamp,
		})
	if err != nil {
		return 0, err
	}
	_ = m.offsetManager.Offset(ctx, id, info.ListOffsetInfo{
		{EventlogID: m.deadLetterEventlogID, Offset: cliOffsets[0].Offset},
	}, true)
	return cliOffsets[0].Offset, nil
}

func (m *manager) SaveDeadLetterOffset(ctx context.Context, id vanus.ID, offset uint64) (err error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return errors.ErrResourceNotFound
	}
	if m.deadLetterEventlogID == 0 {
		logIDs, err := m.getEventlogIDFromCli(ctx,
			primitive.GetDeadLetterEventbusName(subscription.Eventbus))
		if err != nil {
			return err
		}
		m.deadLetterEventlogID = logIDs[0]
	}
	return m.offsetManager.Offset(ctx, id, info.ListOffsetInfo{{
		EventlogID: m.deadLetterEventlogID, Offset: offset,
	}}, true)
}

func (m *manager) getEventlogIDFromCli(ctx context.Context, eventbus string) ([]vanus.ID, error) {
	logs, err := m.ebCli.Eventbus(ctx, eventbus).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	logIDs := make([]vanus.ID, len(logs))
	for i, l := range logs {
		logIDs[i] = vanus.NewIDFromUint64(l.ID())
	}
	return logIDs, nil
}

func (m *manager) getOffsetFromCli(ctx context.Context, eventbus string,
	config primitive.SubscriptionConfig,
) (info.ListOffsetInfo, error) {
	logs, err := m.ebCli.Eventbus(ctx, eventbus).ListLog(ctx)
	if err != nil {
		return nil, err
	}
	offsets := make(info.ListOffsetInfo, len(logs))
	for i, l := range logs {
		var v int64
		switch config.OffsetType {
		case primitive.EarliestOffset:
			if v, err = l.EarliestOffset(ctx); err != nil {
				return nil, err
			}
		case primitive.Timestamp:
			t := config.OffsetTimestamp
			if v, err = l.QueryOffsetByTime(ctx, int64(*t)); err != nil {
				return nil, err
			}
		default:
			if v, err = l.LatestOffset(ctx); err != nil {
				return nil, err
			}
		}
		// fix offset is negative which convert to uint64 is big.
		if v < 0 {
			v = 0
		}
		offsets[i] = info.OffsetInfo{
			EventlogID: vanus.NewIDFromUint64(l.ID()),
			Offset:     uint64(v),
		}
	}
	return offsets, nil
}
