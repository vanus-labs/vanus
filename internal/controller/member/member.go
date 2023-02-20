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

package member

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
)

var (
	ErrStartEtcd            = errors.New("start etcd failed")
	ErrStartEtcdCanceled    = errors.New("etcd start canceled")
	defaultEtcdStartTimeout = 2 * time.Minute
)

type Member interface {
	Init(context.Context, Config) error
	Start(context.Context) (<-chan struct{}, error)
	Stop(context.Context)
	RegisterMembershipChangedProcessor(MembershipEventProcessor)
	ResignIfLeader()
	IsLeader() bool
	GetLeaderID() string
	GetLeaderAddr() string
	IsReady() bool
}

func New(topology map[string]string) *member { //nolint:revive // it's ok
	return &member{
		topology: topology,
	}
}

type LeaderInfo struct {
	LeaderID   string
	LeaderAddr string
}

type EventType string

const (
	EventBecomeLeader   = "leader"
	EventBecomeFollower = "follower"
)

type MembershipChangedEvent struct {
	Type EventType
}

type MembershipEventProcessor func(ctx context.Context, event MembershipChangedEvent) error

type member struct {
	cfg           *Config
	client        *clientv3.Client
	resourcelock  string
	leaseDuration int64
	session       *concurrency.Session
	mutex         *concurrency.Mutex
	isLeader      atomic.Bool
	handlers      []MembershipEventProcessor
	topology      map[string]string
	wg            sync.WaitGroup
	handlerMu     sync.RWMutex
	sessionMu     sync.RWMutex
	exit          chan struct{}
	isReady       atomic.Bool
}

const (
	dialTimeout          = 5
	dialKeepAliveTime    = 1
	dialKeepAliveTimeout = 3
	acquireLockDuration  = 5
)

func (m *member) Init(ctx context.Context, cfg Config) error {
	m.cfg = &cfg
	m.resourcelock = fmt.Sprintf("%s/%s", ResourceLockKeyPrefixInKVStore, cfg.Name)
	m.leaseDuration = cfg.LeaseDuration
	m.exit = make(chan struct{})

	err := m.waitForEtcdReady(ctx, cfg.EtcdEndpoints)
	if err != nil {
		log.Error(context.Background(), "etcd is not ready", nil)
		return err
	}

	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(int(m.leaseDuration)))
	if err != nil {
		log.Error(context.Background(), "new session failed", map[string]interface{}{
			log.KeyError: err,
		})
		panic("new session failed")
	}
	m.mutex = concurrency.NewMutex(m.session, m.resourcelock)
	log.Info(context.Background(), "new leaderelection manager", map[string]interface{}{
		"name":           m.cfg.Name,
		"key":            m.resourcelock,
		"lease_duration": m.leaseDuration,
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
		DialTimeout:          dialTimeout * time.Second,
		DialKeepAliveTime:    dialKeepAliveTime * time.Second,
		DialKeepAliveTimeout: dialKeepAliveTimeout * time.Second,
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

func (m *member) Start(ctx context.Context) (<-chan struct{}, error) {
	log.Info(ctx, "start leaderelection", nil)
	return m.tryAcquireLockLoop(ctx)
}

func (m *member) tryAcquireLockLoop(ctx context.Context) (<-chan struct{}, error) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Warning(ctx, "context canceled at try acquire lock loop", nil)
				return
			case <-m.session.Done():
				log.Warning(ctx, "lose lock", nil)
				m.isLeader.Store(false)
				m.isReady.Store(false)
				_ = m.execHandlers(ctx, MembershipChangedEvent{
					Type: EventBecomeFollower,
				})
				m.refresh(ctx)
			default:
				_ = m.tryLock(ctx)
				time.Sleep(acquireLockDuration * time.Second)
			}
		}
	}()
	log.Info(ctx, "start try to acquire lock loop...", nil)
	return m.exit, nil
}

func (m *member) tryLock(ctx context.Context) error {
	if m.isLeader.Load() {
		return nil
	}
	err := m.mutex.TryLock(ctx)
	if err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			m.isReady.Store(true)
			log.Info(ctx, "try acquire lock, already locked in another session", map[string]interface{}{
				"identity":     m.cfg.Name,
				"resourcelock": m.resourcelock,
			})
			return err
		}
		log.Error(ctx, "acquire lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return err
	}

	log.Info(ctx, "acquired lock", map[string]interface{}{
		"identity":     m.cfg.Name,
		"resourcelock": m.resourcelock,
	})

	err = m.setLeader(ctx)
	if err != nil {
		log.Error(ctx, "set leader info failed", map[string]interface{}{
			"leader_id":   os.Getenv("POD_NAME"),
			"leader_addr": m.topology[os.Getenv("POD_NAME")],
			log.KeyError:  err,
		})
		return err
	}

	m.isLeader.Store(true)
	m.isReady.Store(true)
	log.Info(ctx, "controller become leader", nil)
	_ = m.execHandlers(ctx, MembershipChangedEvent{
		Type: EventBecomeLeader,
	})
	return nil
}

func (m *member) setLeader(ctx context.Context) error {
	data, _ := json.Marshal(&LeaderInfo{
		LeaderID:   os.Getenv("POD_NAME"),
		LeaderAddr: m.topology[os.Getenv("POD_NAME")],
	})
	_, err := m.client.Put(ctx, LeaderInfoKeyPrefixInKVStore, string(data))
	return err
}

func (m *member) refresh(ctx context.Context) {
	var err error
	m.sessionMu.Lock()
	defer m.sessionMu.Unlock()
	m.session.Close()
	m.session, err = concurrency.NewSession(m.client, concurrency.WithTTL(int(m.leaseDuration)))
	if err != nil {
		log.Error(context.Background(), "refresh session failed", map[string]interface{}{
			log.KeyError: err,
		})
		panic("refresh session failed")
	}
	m.mutex = concurrency.NewMutex(m.session, m.resourcelock)
}

func (m *member) Stop(ctx context.Context) {
	log.Info(ctx, "stop leaderelection", nil)
	err := m.release(ctx)
	if err != nil {
		log.Error(ctx, "release lock failed", map[string]interface{}{
			log.KeyError: err,
		})
		return
	}
	m.wg.Wait()
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
			return err
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
