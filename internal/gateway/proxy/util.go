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
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func containsEventbus(list []*metapb.Eventbus, id vanus.ID) bool {
	for _, eb := range list {
		if eb.Id == id.Uint64() {
			return true
		}
	}
	return false
}

func containsSubscription(list []*metapb.Subscription, id vanus.ID) bool {
	for _, sub := range list {
		if sub.Id == id.Uint64() {
			return true
		}
	}
	return false
}
