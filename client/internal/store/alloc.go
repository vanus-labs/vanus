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

package store

import "context"

var allocator = NewAllocator()

// Get acquire BlockStore.
func Get(ctx context.Context, endpoint string) (*BlockStore, error) {
	return allocator.Get(ctx, endpoint)
}

// Put release BlockStore.
func Put(ctx context.Context, bs *BlockStore) {
	allocator.Put(ctx, bs)
}
