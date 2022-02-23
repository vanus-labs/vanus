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
	"errors"
	kvdef "github.com/linkall-labs/vanus/internal/kv"
	v3client "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

var (
	ErrorKeyNotFound = errors.New("key not found")
	ErrorNodeExist   = errors.New("node exist")
	ErrorSetFailed   = errors.New("set failed")
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

func (c *etcdClient3) Get(key string) ([]byte, error) {
	resp, err := c.client.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrorKeyNotFound
	}
	return resp.Kvs[0].Value, nil
}

func (c *etcdClient3) Create(key string, value []byte) error {
	resp, err := c.client.Txn(context.Background()).
		If(v3client.Compare(v3client.CreateRevision(key), "=", 0)).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return ErrorNodeExist
	}
	return nil
}

func (c *etcdClient3) Set(key string, value []byte) error {
	_, err := c.client.Put(context.Background(), key, string(value))
	return err
}

func (c *etcdClient3) Update(key string, value []byte) error {
	resp, err := c.client.Txn(context.Background()).
		If(v3client.Compare(v3client.CreateRevision(key), ">", 0)).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return ErrorKeyNotFound
	}

	return nil
}

func (c *etcdClient3) Exists(key string) (bool, error) {
	resp, err := c.client.Get(context.Background(), key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (c *etcdClient3) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	resp, err := c.client.Grant(context.Background(), ttl.Nanoseconds()/int64(time.Second))
	if err != nil {
		return err
	}
	leaseID := resp.ID
	_, err = c.client.Put(context.Background(), key, string(value), v3client.WithLease(leaseID))
	return err
}

func (c *etcdClient3) Delete(key string) error {
	resp, err := c.client.Delete(context.Background(), key)
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return nil
		//return ErrorKeyNotFound // as need
	}
	return nil
}

func (c *etcdClient3) DeleteDir(path string) error {
	resp, err := c.client.Delete(context.Background(), path, v3client.WithPrefix())
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return nil
		//return ErrorKeyNotFound // as need
	}
	return nil
}

func (c *etcdClient3) List(path string) ([]kvdef.Pair, error) {
	resp, err := c.client.Get(context.Background(), path, v3client.WithPrefix())
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

func (c *etcdClient3) ListKey(path string) (map[string]struct{}, error) {
	path = strings.TrimSuffix(path, "/")
	resp, err := c.client.Get(context.Background(), path, v3client.WithPrefix(), v3client.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, 0)
	for _, kvdefin := range resp.Kvs {
		keys[string(kvdefin.Key)[len(path)+1:]] = struct{}{}
	}
	return keys, nil
}

func (c *etcdClient3) watch(key string, stopCh <-chan struct{}, isTree bool) (chan kvdef.Pair, chan error) {
	watcher := v3client.NewWatcher(c.client)
	var watchC v3client.WatchChan
	if isTree {
		watchC = watcher.Watch(context.Background(), key, v3client.WithPrefix(), v3client.WithPrevKV())
	} else {
		watchC = watcher.Watch(context.Background(), key, v3client.WithPrevKV())
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
						if e.PrevKv != nil {
							pair.Value = e.PrevKv.Value
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

func (c *etcdClient3) Watch(key string, stopCh <-chan struct{}) (chan kvdef.Pair, chan error) {
	return c.watch(key, stopCh, false)
}

func (c *etcdClient3) WatchTree(path string, stopCh <-chan struct{}) (chan kvdef.Pair, chan error) {
	return c.watch(path, stopCh, true)
}

func (c *etcdClient3) CompareAndSwap(key string, preValue, value []byte) error {
	resp, err := c.client.Txn(context.Background()).
		If(v3client.Compare(v3client.Value(key), "=", string(preValue))).
		Then(v3client.OpPut(key, string(value))).
		Else().
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrorSetFailed
	}
	return nil
}

func (c *etcdClient3) CompareAndDelete(key string, preValue []byte) error {
	resp, err := c.client.Txn(context.Background()).
		If(v3client.Compare(v3client.Value(key), "=", string(preValue))).
		Then(v3client.OpDelete(key)).
		Else().
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrorSetFailed
	}
	return nil
}

func (c *etcdClient3) Close() {
	c.client.Close()
}
