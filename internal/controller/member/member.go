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

//go:generate mockgen -source=member.go -destination=mock_member.go -package=member
package member

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"

	"github.com/vanus-labs/vanus/observability/log"
)

var (
	ErrStartEtcd            = errors.New("start etcd failed")
	ErrStartEtcdCanceled    = errors.New("etcd start canceled")
	defaultEtcdStartTimeout = time.Minute
)

type Member interface {
	Init(context.Context) error
	Start(context.Context) error
	Stop(context.Context)
	RegisterMembershipChangedProcessor(MembershipEventProcessor)
	ResignIfLeader()
	IsLeader() bool
	GetLeaderID() string
	GetLeaderAddr() string
	IsReady() bool
}

var _ Member = &member{}

func New(cfg Config) Member {
	return &member{
		cfg: cfg,
	}
}

type LeaderInfo struct {
	LeaderID   string
	LeaderAddr string
}

type EventType string

const (
	EventBecomeLeader   EventType = "leader"
	EventBecomeFollower EventType = "follower"
)

type MembershipChangedEvent struct {
	Type EventType
}

type MembershipEventProcessor func(ctx context.Context, event MembershipChangedEvent) error

type member struct {
	cfg             Config
	client          *clientv3.Client
	resourceLockKey string
	session         *concurrency.Session
	mutex           *concurrency.Mutex
	isLeader        atomic.Bool
	handlers        []MembershipEventProcessor
	handlerMu       sync.RWMutex
	sessionMu       sync.RWMutex
	exit            chan struct{}
	isReady         atomic.Bool
	wg              sync.WaitGroup
}

const (
	dialTimeout          = 5 * time.Second
	dialKeepAliveTime    = 1 * time.Second
	dialKeepAliveTimeout = 3 * time.Second
	acquireLockDuration  = 5 * time.Second
)

func (m *member) Init(ctx context.Context) error {
	m.resourceLockKey = fmt.Sprintf("%s/%s", ResourceLockKeyPrefixInKVStore, m.cfg.ComponentName)
	m.exit = make(chan struct{})

	err := m.waitForEtcdReady(ctx, m.cfg.EtcdEndpoints)
	if err != nil {
		log.Error(ctx, "etcd is not ready", nil)
		return err
	}

	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(m.cfg.LeaseDurationInSecond))
	if err != nil {
		log.Error(ctx, "new session failed", map[string]interface{}{
			log.KeyError: err,
		})
		panic("new session failed")
	}
	m.mutex = concurrency.NewMutex(m.session, m.resourceLockKey)
	log.Info(ctx, "new leaderelection manager", map[string]interface{}{
		"name":           m.cfg.NodeName,
		"key":            m.resourceLockKey,
		"lease_duration": m.cfg.LeaseDurationInSecond,
	})
	return nil
}

func (m *member) waitForEtcdReady(ctx context.Context, endpoints []string) error {
	start := time.Now()
	log.Info(context.Background(), "wait for etcd is ready", nil)
	t := time.NewTicker(defaultEtcdStartTimeout)
	defer t.Stop()
	for !m.ready(ctx, endpoints) {
		select {
		case <-t.C:
			return errors.New("etcd isn't ready")
		default:
			time.Sleep(time.Second)
		}
	}

	log.Info(context.Background(), "etcd is ready", map[string]interface{}{
		"waiting_time": time.Since(start),
	})
	return nil
}

func (m *member) ready(ctx context.Context, endpoints []string) bool {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    dialKeepAliveTime,
		DialKeepAliveTimeout: dialKeepAliveTimeout,
	})
	if err != nil {
		log.Warning(ctx, "new etcd v3client failed", map[string]interface{}{
			log.KeyError: err,
		})
		return false
	}
	m.client = client
	return true
}

func (m *member) Start(_ context.Context) error {
	m.leaderElection()
	return nil
}

func (m *member) leaderElection() {
	ctx := context.Background()
	m.wg.Add(1)
	go func() {
		ticker := time.NewTicker(acquireLockDuration)
		defer func() {
			ticker.Stop()
			m.wg.Done()
		}()
		_ = m.tryLock(ctx) // execute after server start
		for {
		RUN:
			select {
			case <-m.exit:
				log.Info(ctx, "leaderelection has stopped", nil)
				return
			case <-m.session.Done():
				log.Warning(ctx, "lost lock", nil)
				m.isLeader.Store(false)
				m.isReady.Store(false)
				_ = m.execHandlers(ctx, MembershipChangedEvent{
					Type: EventBecomeFollower,
				})
				// refresh session until success
				t := time.NewTicker(time.Second)
				for {
					select {
					case <-m.exit:
						goto RUN
					case <-t.C:
						if m.refresh(ctx) {
							goto RUN
						}
					}
				}
			case <-ticker.C:
				_ = m.tryLock(ctx)
			}
		}
	}()
	log.Info(ctx, "leaderelection has started", nil)
}

func (m *member) tryLock(ctx context.Context) error {
	if m.isLeader.Load() {
		return nil
	}
	err := m.mutex.TryLock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			m.isReady.Store(true)
			log.Debug(ctx, "try acquire lock, already locked in another session", map[string]interface{}{
				"identity":      m.cfg.NodeName,
				"resource_lock": m.resourceLockKey,
			})
			return err
		}
		log.Error(ctx, "acquire lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	log.Info(ctx, "success to acquire distributed lock", map[string]interface{}{
		"identity":      m.cfg.NodeName,
		"resource_lock": m.resourceLockKey,
	})

	err = m.setLeader(ctx)
	if err != nil {
		log.Error(ctx, "failed to set leader info", map[string]interface{}{
			"leader_id":   m.cfg.NodeName,
			"leader_addr": m.cfg.Topology[m.cfg.NodeName],
			log.KeyError:  err,
		})
		_ = m.mutex.Unlock(ctx)
		return err
	}

	log.Info(ctx, "controller become leader", nil)
	_ = m.execHandlers(ctx, MembershipChangedEvent{
		Type: EventBecomeLeader,
	})
	m.isLeader.Store(true)
	m.isReady.Store(true)
	return nil
}

func (m *member) setLeader(ctx context.Context) error {
	data, _ := json.Marshal(&LeaderInfo{
		LeaderID:   m.cfg.NodeName,
		LeaderAddr: m.cfg.Topology[m.cfg.NodeName],
	})
	_, err := m.client.Put(ctx, LeaderInfoKeyPrefixInKVStore, string(data))
	return err
}

func (m *member) refresh(ctx context.Context) bool {
	var err error
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	_ = m.session.Close()
	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(m.cfg.LeaseDurationInSecond))
	if err != nil {
		log.Error(ctx, "refresh session failed", map[string]interface{}{
			log.KeyError: err,
		})
		return false
	}
	m.mutex = concurrency.NewMutex(m.session, m.resourceLockKey)
	return true
}

func (m *member) Stop(ctx context.Context) {
	log.Info(ctx, "stop leaderelection", nil)
	close(m.exit)
	m.wg.Wait()
	err := m.release(ctx)
	if err != nil {
		log.Error(ctx, "release lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return
	}
}

func (m *member) release(ctx context.Context) error {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	err := m.mutex.Unlock(ctx)
	if err != nil {
		log.Error(ctx, "unlock error", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	err = m.session.Close()
	if err != nil {
		log.Error(ctx, "session close error", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}
	log.Info(ctx, "released lock", nil)
	return nil
}

func (m *member) execHandlers(ctx context.Context, event MembershipChangedEvent) error {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	for _, handler := range m.handlers {
		err := handler(ctx, event)
		if err != nil {
			log.Error(ctx, "exec handler failed and exit", map[string]interface{}{
				log.KeyError: err,
			})
			panic("exec handler failed")
		}
	}
	return nil
}

func (m *member) RegisterMembershipChangedProcessor(handler MembershipEventProcessor) {
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.handlers = append(m.handlers, handler)
}

func (m *member) ResignIfLeader() {
	// TODO(jiangkai)
}

func (m *member) IsLeader() bool {
	return m.isLeader.Load()
}

func (m *member) GetLeaderID() string {
	// TODO(jiangkai): maybe lookup etcd per call has low performance.
	resp, err := m.client.Get(context.Background(), LeaderInfoKeyPrefixInKVStore)
	if err != nil {
		log.Warning(context.Background(), "get leader info failed", map[string]interface{}{
			log.KeyError: err,
		})
		return ""
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	leader := &LeaderInfo{}
	_ = json.Unmarshal(resp.Kvs[0].Value, leader)
	return leader.LeaderID
}

func (m *member) GetLeaderAddr() string {
	// TODO(jiangkai): maybe lookup etcd per call has low performance.
	resp, err := m.client.Get(context.Background(), LeaderInfoKeyPrefixInKVStore)
	if err != nil {
		log.Warning(context.Background(), "get leader info failed", map[string]interface{}{
			log.KeyError: err,
		})
		return ""
	}
	if len(resp.Kvs) == 0 {
		return ""
	}
	leader := &LeaderInfo{}
	_ = json.Unmarshal(resp.Kvs[0].Value, leader)
	return leader.LeaderAddr
}

func (m *member) IsReady() bool {
	return m.isReady.Load()
}
