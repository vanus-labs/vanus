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

package kv

import (
	"context"
	"errors"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrNodeExist   = errors.New("node exist")
	ErrSetFailed   = errors.New("set failed")
	ErrUnknown     = errors.New("unknown")
)

type Client interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Create(ctx context.Context, key string, value []byte) error
	Set(ctx context.Context, key string, value []byte) error
	Update(ctx context.Context, key string, value []byte) error
	Exists(ctx context.Context, key string) (bool, error)
	SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	DeleteDir(ctx context.Context, path string) error
	List(ctx context.Context, path string) ([]Pair, error)
	Watch(ctx context.Context, key string, stopCh <-chan struct{}) (chan Pair, chan error)
	WatchTree(ctx context.Context, path string, stopCh <-chan struct{}) (chan Pair, chan error)
	CompareAndSwap(ctx context.Context, key string, preValue, value []byte) error
	CompareAndDelete(ctx context.Context, key string, preValue []byte) error
	Close() error
}

type Pair struct {
	Key    string
	Value  []byte
	Action Action
}

type Action string

const (
	Create Action = "create"
	Delete Action = "delete"
	Update Action = "update"
)
