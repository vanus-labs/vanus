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
)

type fake struct {
	subs   map[string]*primitive.Subscription
	offset map[string]map[string]int64
}

func NewFakeStorage(config primitive.KvStorageConfig) (Storage, error) {
	s := &fake{
		subs:   map[string]*primitive.Subscription{},
		offset: map[string]map[string]int64{},
	}
	return s, nil
}

func (f *fake) Close() {

}

func (f *fake) CreateSubscription(ctx context.Context, sub *primitive.Subscription) error {
	f.subs[sub.ID] = sub
	return nil
}

func (f *fake) DeleteSubscription(ctx context.Context, id string) error {
	delete(f.subs, id)
	return nil
}

func (f *fake) GetSubscription(ctx context.Context, id string) (*primitive.Subscription, error) {
	return f.subs[id], nil
}

func (f *fake) ListSubscription(ctx context.Context) ([]*primitive.Subscription, error) {
	var list []*primitive.Subscription
	for _, sub := range f.subs {
		list = append(list, sub)
	}
	return list, nil
}

func (f *fake) CreateOffset(ctx context.Context, info *info.OffsetInfo) error {
	v, exist := f.offset[info.SubId]
	if !exist {
		v = map[string]int64{}
		f.offset[info.SubId] = v
	}
	v[info.EventLog] = info.Offset
	return nil
}
func (f *fake) UpdateOffset(ctx context.Context, info *info.OffsetInfo) error {
	v, exist := f.offset[info.SubId]
	if !exist {
		v = map[string]int64{}
		f.offset[info.SubId] = v
	}
	v[info.EventLog] = info.Offset
	return nil
}
func (f *fake) GetOffset(ctx context.Context, info *info.OffsetInfo) (int64, error) {
	v, exist := f.offset[info.SubId]
	if !exist {
		return 0, kv.ErrorKeyNotFound
	}
	o, exist := v[info.EventLog]
	if !exist {
		return 0, kv.ErrorKeyNotFound
	}
	return o, nil
}
func (f *fake) ListOffset(ctx context.Context, subId string) ([]*info.OffsetInfo, error) {
	return nil, nil
}

func (f *fake) DeleteOffset(ctx context.Context, id string) error {
	return nil
}
