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

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

func (m *manager) SaveOffset(ctx context.Context, id vanus.ID, offsets info.ListOffsetInfo, commit bool) error {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return nil
	}
	return m.offsetManager.Offset(ctx, id, offsets, commit)
}

func (m *manager) ResetOffsetByTimestamp(ctx context.Context, id vanus.ID,
	timestamp uint64) (info.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return nil, ErrSubscriptionNotExist
	}
	offsets, err := m.getOffsetFromCli(ctx, subscription.EventBus, primitive.SubscriptionConfig{
		OffsetTimestamp: &timestamp,
		OffsetType:      primitive.Timestamp,
	}, false)
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
		return info.ListOffsetInfo{}, ErrSubscriptionNotExist
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return nil, err
	}
	// todo filter retry eventlog
	return offsets, nil
}

func (m *manager) GetOrSaveOffset(ctx context.Context, id vanus.ID) (info.ListOffsetInfo, error) {
	subscription := m.GetSubscription(ctx, id)
	if subscription == nil {
		return info.ListOffsetInfo{}, ErrSubscriptionNotExist
	}
	offsets, err := m.offsetManager.GetOffset(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(offsets) > 0 {
		return offsets, nil
	}

	offsets, err = m.getOffsetFromCli(ctx, subscription.EventBus, subscription.Config, false)
	if err != nil {
		return nil, err
	}
	// get retry eb offset.
	retryOffset, err := m.getOffsetFromCli(ctx, primitive.RetryEventbusName, primitive.SubscriptionConfig{
		OffsetType: primitive.LatestOffset,
	}, true)
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

func (m *manager) getOffsetFromCli(ctx context.Context, eventbus string,
	config primitive.SubscriptionConfig, retryEventBus bool) (info.ListOffsetInfo, error) {
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
			EventLogID: vanus.NewIDFromUint64(l.ID()),
			Offset:     uint64(v),
		}
	}
	return offsets, nil
}
