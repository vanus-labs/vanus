/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcelock

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
)

type LeaseLock struct {
	identity   string
	leaseName  string
	etcdClient kv.Client
}

// func init() {
// 	etcdClient, err := etcd.NewEtcdClientV3([]string{"192.168.49.2:30007"}, "/vanus")
// 	if err != nil {
// 		panic(err)
// 	}
// }

// Get returns the election record from a Lease spec
func (ll *LeaseLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
	v, err := ll.etcdClient.Get(ctx, fmt.Sprintf("%s/%s", metadata.ResourceLockKeyPrefixInKVStore, ll.leaseName))
	if err != nil {
		return nil, []byte{}, err
	}
	ler := &LeaderElectionRecord{}
	err = json.Unmarshal(v, ler)
	if err != nil {
		return nil, []byte{}, err
	}
	return ler, v, nil
}

// Create attempts to create a Lease
func (ll *LeaseLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
	body, err := json.Marshal(ler)
	if err != nil {
		return err
	}
	return ll.etcdClient.Create(ctx, fmt.Sprintf("%s/%s", metadata.ResourceLockKeyPrefixInKVStore, ll.leaseName), body)
}

// Update will update an existing Lease spec.
func (ll *LeaseLock) Update(ctx context.Context, ler LeaderElectionRecord) error {
	body, err := json.Marshal(ler)
	if err != nil {
		return err
	}
	return ll.etcdClient.Update(ctx, fmt.Sprintf("%s/%s", metadata.ResourceLockKeyPrefixInKVStore, ll.leaseName), body)
}

// RecordEvent in leader election while adding meta-data
func (ll *LeaseLock) RecordEvent(s string) {
}

// Identity returns the Identity of the lock
func (ll *LeaseLock) Identity() string {
	return ll.identity
}

// Describe is used to convert details on current resource lock
// into a string
func (ll *LeaseLock) Describe() string {
	return ll.leaseName
}
