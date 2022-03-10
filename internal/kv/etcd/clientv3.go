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

package etcd

import (
	"context"
	kvdef "github.com/linkall-labs/vanus/internal/kv"
	v3client "go.etcd.io/etcd/client/v3"
	"path"
	"strings"
	"time"
)

type etcdClient3 struct {
	client    *v3client.Client
	keyPrefix string
}

func NewEtcdClientV3(endpoints []string, keyPrefix string) (*etcdClient3, error) {
	client, err := v3client.New(v3client.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		//DialKeepAliveTime:    1 * time.Second,
		//DialKeepAliveTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &etcdClient3{client: client, keyPrefix: keyPrefix}, nil
}

func (c *etcdClient3) Get(ctx context.Context, key string) ([]byte, error) {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, kvdef.ErrorKeyNotFound
	}
	return resp.Kvs[0].Value, nil
}

func (c *etcdClient3) Create(ctx context.Context, key string, value []byte) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Txn(ctx).
		If(v3client.Compare(v3client.CreateRevision(key), "=", 0)).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return kvdef.ErrorNodeExist
	}
	return nil
}

func (c *etcdClient3) Set(ctx context.Context, key string, value []byte) error {
	key = path.Join(c.keyPrefix, key)
	_, err := c.client.Put(ctx, key, string(value))
	return err
}

func (c *etcdClient3) Update(ctx context.Context, key string, value []byte) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Txn(ctx).
		If(v3client.Compare(v3client.CreateRevision(key), ">", 0)).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return kvdef.ErrorKeyNotFound
	}

	return nil
}

func (c *etcdClient3) Exists(ctx context.Context, key string) (bool, error) {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (c *etcdClient3) SetWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Grant(ctx, ttl.Nanoseconds()/int64(time.Second))
	if err != nil {
		return err
	}
	leaseID := resp.ID
	_, err = c.client.Put(ctx, key, string(value), v3client.WithLease(leaseID))
	return err
}

func (c *etcdClient3) Delete(ctx context.Context, key string) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Delete(ctx, key)
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return nil
		//return ErrorKeyNotFound // as need
	}
	return nil
}

func (c *etcdClient3) DeleteDir(ctx context.Context, key string) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Delete(ctx, key, v3client.WithPrefix())
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return nil
		//return ErrorKeyNotFound // as need
	}
	return nil
}

func (c *etcdClient3) List(ctx context.Context, key string) ([]kvdef.Pair, error) {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Get(ctx, key, v3client.WithPrefix())
	if err != nil {
		return nil, err
	}
	pairs := make([]kvdef.Pair, 0)
	for _, kvdefin := range resp.Kvs {
		pairs = append(pairs, kvdef.Pair{
			Key:   string(kvdefin.Key),
			Value: kvdefin.Value,
		})
	}
	return pairs, nil
}

func (c *etcdClient3) ListKey(ctx context.Context, path string) (map[string]struct{}, error) {
	path = strings.TrimSuffix(path, "/")
	resp, err := c.client.Get(ctx, path, v3client.WithPrefix(), v3client.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, 0)
	for _, kvdefin := range resp.Kvs {
		keys[string(kvdefin.Key)[len(path)+1:]] = struct{}{}
	}
	return keys, nil
}

func (c *etcdClient3) watch(ctx context.Context, key string, stopCh <-chan struct{}, isTree bool) (chan kvdef.Pair, chan error) {
	watcher := v3client.NewWatcher(c.client)
	var watchC v3client.WatchChan
	if isTree {
		watchC = watcher.Watch(ctx, key, v3client.WithPrefix(), v3client.WithPrevKV())
	} else {
		watchC = watcher.Watch(ctx, key, v3client.WithPrevKV())
	}
	pairC := make(chan kvdef.Pair, 100)
	errorC := make(chan error, 10)
	go func() {
		for {
			select {
			case es := <-watchC:
				if err := es.Err(); err != nil {
					errorC <- err
					watcher.Close()
					return
				}
				for _, e := range es.Events {
					pair := kvdef.Pair{
						Key:   string(e.Kv.Key),
						Value: e.Kv.Value,
					}
					if e.Type == v3client.EventTypeDelete {
						pair.Action = kvdef.Delete
					} else {
						if e.IsCreate() {
							pair.Action = kvdef.Create
						} else {
							pair.Action = kvdef.Update
						}
					}
					pairC <- pair
				}
			case <-stopCh:
				watcher.Close()
				return
			}
		}
	}()
	return pairC, errorC
}

func (c *etcdClient3) Watch(ctx context.Context, key string, stopCh <-chan struct{}) (chan kvdef.Pair, chan error) {
	key = path.Join(c.keyPrefix, key)
	return c.watch(ctx, key, stopCh, false)
}

func (c *etcdClient3) WatchTree(ctx context.Context, key string, stopCh <-chan struct{}) (chan kvdef.Pair, chan error) {
	key = path.Join(c.keyPrefix, key)
	return c.watch(ctx, key, stopCh, true)
}

func (c *etcdClient3) CompareAndSwap(ctx context.Context, key string, preValue, value []byte) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Txn(ctx).
		If(v3client.Compare(v3client.Value(key), "=", string(preValue))).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return kvdef.ErrorSetFailed
	}
	return nil
}

func (c *etcdClient3) CompareAndDelete(ctx context.Context, key string, preValue []byte) error {
	key = path.Join(c.keyPrefix, key)
	resp, err := c.client.Txn(ctx).
		If(v3client.Compare(v3client.Value(key), "=", string(preValue))).
		Then(v3client.OpDelete(key)).
		Else().
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return kvdef.ErrorSetFailed
	}
	return nil
}

func (c *etcdClient3) Close() error {
	return c.client.Close()
}
