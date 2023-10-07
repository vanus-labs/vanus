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
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"

	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/pkg/observability/log"
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
	m.resourceLockKey = kv.DistributedLockKey(m.cfg.ComponentName)
	m.exit = make(chan struct{})

	err := m.waitForEtcdReady(ctx, m.cfg.EtcdEndpoints)
	if err != nil {
		log.Error(ctx).Msg("etcd is not ready")
		return err
	}

	log.Info(ctx).Msg("try to create session")
	start := time.Now()
	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(m.cfg.LeaseDurationInSecond))
	if err != nil {
		log.Error(ctx).Err(err).Msg("new session failed")
		panic("new session failed")
	}
	log.Info(ctx).
		Dur("duration", time.Since(start)).
		Msg("create session is finished")

	m.mutex = concurrency.NewMutex(m.session, m.resourceLockKey)
	log.Info(ctx).
		Str("name", m.cfg.NodeName).
		Str("key", m.resourceLockKey).
		Int("lease_duration", m.cfg.LeaseDurationInSecond).
		Msg("new leaderelection manager")
	return nil
}

func (m *member) waitForEtcdReady(ctx context.Context, endpoints []string) error {
	start := time.Now()
	log.Info().Msg("wait for etcd is ready")
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

	log.Info().Dur("waiting_time", time.Since(start)).Msg("etcd is ready")
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
		log.Warn(ctx).Err(err).Msg("new etcd v3client failed")
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
				log.Info(ctx).Msg("leaderelection has stopped")
				return
			case <-m.session.Done():
				log.Warn(ctx).Msg("lost lock")
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
							t.Stop()
							goto RUN
						}
					}
				}
			case <-ticker.C:
				_ = m.tryLock(ctx)
			}
		}
	}()
	log.Info(ctx).Msg("leaderelection has started")
}

func (m *member) tryLock(ctx context.Context) error {
	if m.isLeader.Load() {
		return nil
	}
	err := m.mutex.TryLock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			m.isReady.Store(true)
			log.Debug(ctx).
				Str("identity", m.cfg.NodeName).
				Str("resource_lock", m.resourceLockKey).
				Msg("try acquire lock, already locked in another session")
			return err
		}
		log.Error(ctx).Err(err).Msg("acquire lock failed")
		return err
	}

	log.Info(ctx).
		Str("identity", m.cfg.NodeName).
		Str("resource_lock", m.resourceLockKey).
		Msg("success to acquire distributed lock")

	err = m.setLeader(ctx)
	if err != nil {
		log.Error(ctx).Err(err).
			Str("identity", m.cfg.NodeName).
			Str("leader_addr", m.cfg.Topology[m.cfg.NodeName]).
			Msg("failed to set leader info")
		_ = m.mutex.Unlock(ctx)
		return err
	}

	m.isLeader.Store(true)
	log.Info(ctx).Msg("controller become leader")
	_ = m.execHandlers(ctx, MembershipChangedEvent{
		Type: EventBecomeLeader,
	})
	m.isReady.Store(true)
	return nil
}

func (m *member) setLeader(ctx context.Context) error {
	data, _ := json.Marshal(&LeaderInfo{
		LeaderID:   m.cfg.NodeName,
		LeaderAddr: m.cfg.Topology[m.cfg.NodeName],
	})
	_, err := m.client.Put(ctx, kv.ComponentLeaderKey(m.cfg.ComponentName), string(data))
	return err
}

func (m *member) refresh(ctx context.Context) bool {
	var err error
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	_ = m.session.Close()
	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(m.cfg.LeaseDurationInSecond))
	if err != nil {
		log.Error(ctx).Err(err).Msg("refresh session failed")
		return false
	}
	m.mutex = concurrency.NewMutex(m.session, m.resourceLockKey)
	return true
}

func (m *member) Stop(ctx context.Context) {
	log.Info(ctx).Msg("stop leaderelection")
	close(m.exit)
	m.wg.Wait()
	err := m.release(ctx)
	if err != nil {
		log.Error(ctx).Err(err).Msg("release lock failed")
		return
	}
}

func (m *member) release(ctx context.Context) error {
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	err := m.mutex.Unlock(ctx)
	if err != nil {
		log.Error(ctx).Err(err).Msg("unlock error")
		return err
	}
	err = m.session.Close()
	if err != nil {
		log.Error(ctx).Err(err).Msg("session close error")
		return err
	}
	log.Info(ctx).Msg("released lock")
	return nil
}

func (m *member) execHandlers(ctx context.Context, event MembershipChangedEvent) error {
	m.handlerMu.RLock()
	defer m.handlerMu.RUnlock()
	start := time.Now()
	log.Debug(ctx).Interface("event", event).Msg("start to call handlers")
	for _, handler := range m.handlers {
		err := handler(ctx, event)
		if err != nil {
			log.Error(ctx).
				Err(err).
				Msg("exec handler failed and exit")
			panic("exec handler failed")
		}
	}
	log.Debug(ctx).Interface("event", event).
		Dur("duration", time.Since(start)).
		Msg("finish call handlers")
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
	resp, err := m.client.Get(context.Background(), kv.ComponentLeaderKey(m.cfg.ComponentName))
	if err != nil {
		log.Warn().Err(err).Msg("get leader info failed")
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
	resp, err := m.client.Get(context.Background(), kv.ComponentLeaderKey(m.cfg.ComponentName))
	if err != nil {
		log.Warn().Err(err).Msg("get leader info failed")
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
