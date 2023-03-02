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

//go:generate mockgen -source=leaderelection.go  -destination=mock_leaderelection.go -package=leaderelection
package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/timer/metadata"
	"github.com/linkall-labs/vanus/observability/log"
	"go.uber.org/atomic"

	v3client "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	dialTimeout          = 5
	dialKeepAliveTime    = 1
	dialKeepAliveTimeout = 3
	acquireLockDuration  = 5
)

var (
	newV3Client = v3client.New
	newSession  = concurrency.NewSession
	newMutex    = concurrency.NewMutex
)

type Manager interface {
	Start(ctx context.Context, callbacks LeaderCallbacks) error
	Stop(ctx context.Context) error
	// IsLeader() bool
}

type Mutex interface {
	TryLock(ctx context.Context) error
	Unlock(ctx context.Context) error
}

type leaderElection struct {
	name          string
	resourceLock  string
	leaseDuration int64
	isLeader      atomic.Bool

	etcdClient *v3client.Client
	callbacks  LeaderCallbacks
	session    *concurrency.Session
	mutex      Mutex
	mu         sync.RWMutex
	wg         sync.WaitGroup
}

type LeaderCallbacks struct {
	// OnStartedLeading is called when starts leading
	OnStartedLeading func(context.Context)
	// OnStoppedLeading is called when stops leading
	OnStoppedLeading func(context.Context)
}

func NewLeaderElection(c *Config) Manager {
	var err error
	client, err := newV3Client(v3client.Config{
		Endpoints:            c.EtcdEndpoints,
		DialTimeout:          dialTimeout * time.Second,
		DialKeepAliveTime:    dialKeepAliveTime * time.Second,
		DialKeepAliveTimeout: dialKeepAliveTimeout * time.Second,
	})
	if err != nil {
		log.Error(context.Background(), "new etcd v3client failed", map[string]interface{}{
			log.KeyError: err,
		})
		panic("new etcd v3client failed")
	}

	le := &leaderElection{
		name:          c.Name,
		resourceLock:  fmt.Sprintf("%s/%s", metadata.ResourceLockKeyPrefixInKVStore, c.Name),
		leaseDuration: c.LeaseDuration,
		etcdClient:    client,
	}

	le.session, err = newSession(client, concurrency.WithTTL(int(le.leaseDuration)))
	if err != nil {
		log.Error(context.Background(), "new session failed", map[string]interface{}{
			log.KeyError: err,
		})
		panic("new session failed")
	}
	le.mutex = newMutex(le.session, le.resourceLock)

	log.Info(context.Background(), "new leaderelection manager", map[string]interface{}{
		"name":           le.name,
		"resource_lock":  le.resourceLock,
		"lease_duration": le.leaseDuration,
	})
	return le
}

func (le *leaderElection) Start(ctx context.Context, callbacks LeaderCallbacks) error {
	log.Info(ctx, "start leaderelection", nil)
	le.callbacks = callbacks
	return le.tryAcquireLockLoop(ctx)
}

func (le *leaderElection) Stop(ctx context.Context) error {
	log.Info(ctx, "stop leaderelection", nil)
	err := le.release(ctx)
	if err != nil {
		log.Error(ctx, "release lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	le.isLeader.Store(false)
	le.callbacks.OnStoppedLeading(ctx)
	le.wg.Wait()
	return nil
}

// func (le *leaderElection) IsLeader() bool {
// 	return le.isLeader
// }

func (le *leaderElection) tryAcquireLockLoop(ctx context.Context) error {
	le.wg.Add(1)
	go func() {
		defer le.wg.Done()
		ticker := time.NewTicker(acquireLockDuration * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Warning(ctx, "context canceled at try acquire lock loop", nil)
				return
			case <-le.session.Done():
				log.Warning(ctx, "lose lock", nil)
				le.isLeader.Store(false)
				le.callbacks.OnStoppedLeading(ctx)
				// refresh session until success
				for {
					if le.refresh(ctx) {
						break
					}
					time.Sleep(time.Second)
				}
			case <-ticker.C:
				_ = le.tryLock(ctx)
			}
		}
	}()
	log.Info(ctx, "start try to acquire lock loop...", nil)
	return nil
}

func (le *leaderElection) tryLock(ctx context.Context) error {
	if le.isLeader.Load() {
		return nil
	}
	err := le.mutex.TryLock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			log.Info(ctx, "try acquire lock, already locked in another session", nil)
			return err
		}
		log.Error(ctx, "acquire lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	log.Info(ctx, "acquired lock", map[string]interface{}{
		"identity":      le.name,
		"resource_lock": le.resourceLock,
	})
	le.isLeader.Store(true)
	le.callbacks.OnStartedLeading(ctx)
	return nil
}

func (le *leaderElection) release(ctx context.Context) error {
	le.mu.Lock()
	defer le.mu.Unlock()
	err := le.mutex.Unlock(ctx)
	if err != nil {
		log.Error(ctx, "unlock error", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	err = le.session.Close()
	if err != nil {
		log.Error(ctx, "session close error", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	log.Info(ctx, "released lock", nil)
	return nil
}

func (le *leaderElection) refresh(ctx context.Context) bool {
	var err error
	le.mu.Lock()
	defer le.mu.Unlock()
	le.session.Close()
	le.session, err = concurrency.NewSession(le.etcdClient, concurrency.WithTTL(int(le.leaseDuration)))
	if err != nil {
		log.Error(context.Background(), "refresh session failed", map[string]interface{}{
			log.KeyError: err,
		})
		return false
	}
	le.mutex = concurrency.NewMutex(le.session, le.resourceLock)
	return true
}
