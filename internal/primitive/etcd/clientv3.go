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
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

var (
	ErrorKeyNotFound = errors.New("key not found")
	ErrorNodeExist   = errors.New("node exist")
	ErrorSetFailed   = errors.New("set failed")
)

type etcdClient3 struct {
	client *clientv3.Client
}

func NewEtcdClient3(endpoints []string) (*etcdClient3, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		//DialKeepAliveTime:    1 * time.Second,
		//DialKeepAliveTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &etcdClient3{client: client}, nil
}

func (c *etcdClient3) Get(key string) (string, error) {
	resp, err := c.client.Get(context.Background(), key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", ErrorKeyNotFound
	}
	return string(resp.Kvs[0].Value), nil
}

func (c *etcdClient3) Create(key, value string) error {
	resp, err := c.client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
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

func (c *etcdClient3) Set(key, value string) error {
	_, err := c.client.Put(context.Background(), key, value)
	return err
}

func (c *etcdClient3) Update(key, value string) error {
	resp, err := c.client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(key), ">", 0)).
		Then(clientv3.OpPut(key, value)).
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

func (c *etcdClient3) SetWithTTL(key, value string, ttl time.Duration) error {
	resp, err := c.client.Grant(context.Background(), ttl.Nanoseconds()/int64(time.Second))
	if err != nil {
		return err
	}
	leaseID := resp.ID
	_, err = c.client.Put(context.Background(), key, value, clientv3.WithLease(leaseID))
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
	resp, err := c.client.Delete(context.Background(), path, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return nil
		//return ErrorKeyNotFound // as need
	}
	return nil
}

func (c *etcdClient3) List(path string) ([]Pair, error) {
	resp, err := c.client.Get(context.Background(), path, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	pairs := make([]Pair, 0)
	for _, kv := range resp.Kvs {
		pairs = append(pairs, Pair{
			Key:   string(kv.Key),
			Value: string(kv.Value),
		})
	}
	return pairs, nil
}

func (c *etcdClient3) ListKey(path string) (map[string]struct{}, error) {
	path = strings.TrimSuffix(path, "/")
	resp, err := c.client.Get(context.Background(), path, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{}, 0)
	for _, kv := range resp.Kvs {
		keys[string(kv.Key)[len(path)+1:]] = struct{}{}
	}
	return keys, nil
}

func (c *etcdClient3) watch(key string, stopCh <-chan struct{}, isTree bool) (chan Pair, chan error) {
	watcher := clientv3.NewWatcher(c.client)
	var watchC clientv3.WatchChan
	if isTree {
		watchC = watcher.Watch(context.Background(), key, clientv3.WithPrefix(), clientv3.WithPrevKV())
	} else {
		watchC = watcher.Watch(context.Background(), key, clientv3.WithPrevKV())
	}
	pairC := make(chan Pair, 100)
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
					pair := Pair{
						Key:   string(e.Kv.Key),
						Value: string(e.Kv.Value),
					}
					if e.Type == clientv3.EventTypeDelete {
						if e.PrevKv != nil {
							pair.Value = string(e.PrevKv.Value)
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

func (c *etcdClient3) Watch(key string, stopCh <-chan struct{}) (chan Pair, chan error) {
	return c.watch(key, stopCh, false)
}

func (c *etcdClient3) WatchTree(path string, stopCh <-chan struct{}) (chan Pair, chan error) {
	return c.watch(path, stopCh, true)
}

func (c *etcdClient3) CompareAndSwap(key, preValue, value string) error {
	resp, err := c.client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.Value(key), "=", preValue)).
		Then(clientv3.OpPut(key, value)).
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

func (c *etcdClient3) CompareAndDelete(key, preValue string) error {
	resp, err := c.client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.Value(key), "=", preValue)).
		Then(clientv3.OpDelete(key)).
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
