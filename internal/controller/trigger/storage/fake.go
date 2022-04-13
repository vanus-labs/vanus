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

package storage

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
)

type fake struct {
	subs     map[string]*primitive.SubscriptionApi
	offset   map[string]map[string]pInfo.OffsetInfo
	tWorkers map[string]*info.TriggerWorkerInfo
}

func NewFakeStorage() Storage {
	s := &fake{
		subs:     map[string]*primitive.SubscriptionApi{},
		offset:   map[string]map[string]pInfo.OffsetInfo{},
		tWorkers: map[string]*info.TriggerWorkerInfo{},
	}
	return s
}

func (f *fake) Close() {

}

func (f *fake) CreateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	f.subs[sub.ID] = sub
	return nil
}

func (f *fake) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	f.subs[sub.ID] = sub
	return nil
}

func (f *fake) DeleteSubscription(ctx context.Context, id string) error {
	delete(f.subs, id)
	return nil
}

func (f *fake) GetSubscription(ctx context.Context, id string) (*primitive.SubscriptionApi, error) {
	return f.subs[id], nil
}

func (f *fake) ListSubscription(ctx context.Context) ([]*primitive.SubscriptionApi, error) {
	var list []*primitive.SubscriptionApi
	for _, sub := range f.subs {
		list = append(list, sub)
	}
	return list, nil
}

func (f *fake) CreateOffset(ctx context.Context, subId string, info pInfo.OffsetInfo) error {
	sub, exist := f.offset[subId]
	if !exist {
		sub = map[string]pInfo.OffsetInfo{}
		f.offset[subId] = sub
	}
	sub[info.EventLogId] = info
	return nil
}
func (f *fake) UpdateOffset(ctx context.Context, subId string, info pInfo.OffsetInfo) error {
	sub, exist := f.offset[subId]
	if !exist {
		return kv.ErrorKeyNotFound
	}
	sub[info.EventLogId] = info
	return nil
}
func (f *fake) GetOffsets(ctx context.Context, subId string) (pInfo.ListOffsetInfo, error) {
	sub, exist := f.offset[subId]
	if !exist {
		return nil, nil
	}
	var infos pInfo.ListOffsetInfo
	for _, v := range sub {
		infos = append(infos, v)
	}
	return infos, nil
}

func (f *fake) DeleteOffset(ctx context.Context, subId string) error {
	delete(f.offset, subId)
	return nil
}

func (f *fake) SaveTriggerWorker(ctx context.Context, info info.TriggerWorkerInfo) error {
	f.tWorkers[info.Id] = &info
	return nil
}
func (f *fake) GetTriggerWorker(ctx context.Context, id string) (*info.TriggerWorkerInfo, error) {
	return f.tWorkers[id], nil
}
func (f *fake) DeleteTriggerWorker(ctx context.Context, id string) error {
	delete(f.tWorkers, id)
	return nil
}
func (f *fake) ListTriggerWorker(ctx context.Context) ([]*info.TriggerWorkerInfo, error) {
	var list []*info.TriggerWorkerInfo
	for _, data := range f.tWorkers {
		list = append(list, data)
	}
	return list, nil
}
