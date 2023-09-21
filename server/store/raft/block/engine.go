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

package block

import (
	// standard libraries.
	"container/list"
	"context"
	"sync"
	"time"

	// third-party libraries.
	"google.golang.org/grpc"

	raftpb "github.com/vanus-labs/vanus/api/raft"
	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/pkg/raft"

	// this project.
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/server/store/block"
	"github.com/vanus-labs/vanus/server/store/meta"
	"github.com/vanus-labs/vanus/server/store/raft/storage"
	"github.com/vanus-labs/vanus/server/store/raft/transport"
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

type Engine struct {
	mu        sync.RWMutex
	appenders list.List

	closeC chan struct{}

	wal        *storage.WAL
	stateStore *meta.SyncStore
	hintStore  *meta.AsyncStore

	resolver *transport.SimpleResolver
	host     transport.Host

	executor *executorFactory

	leaderLis LeaderChangedListener
	appendLis EntryAppendedListener

	dir     string
	walOpts []walog.Option
}

func (e *Engine) Init(dir string, localAddr string, opts ...Option) *Engine {
	cfg := makeConfig(opts...)
	return e.init(dir, localAddr, cfg)
}

func (e *Engine) init(dir string, localAddr string, cfg config) *Engine {
	e.appenders.Init()
	e.closeC = make(chan struct{})

	e.dir = dir
	e.walOpts = cfg.walOpts
	e.leaderLis = cfg.leaderLis
	e.appendLis = cfg.appendLis

	e.stateStore = cfg.stateStore
	e.hintStore = cfg.hintStore

	e.resolver = transport.NewSimpleResolver()
	e.host = transport.NewHost(e.resolver, localAddr)

	// TODO(james.yin): lazy starting.
	e.executor = newExecutorFactory(cfg.executorCfg, true)
	go e.runTick()

	return e
}

func (e *Engine) Recover(ctx context.Context, raws map[vanus.ID]block.Raw) (map[vanus.ID]Appender, error) {
	// Recover wal and raft storages.
	storages, wal, err := storage.Recover(ctx, e.dir, e.stateStore, e.hintStore, e.walOpts...)
	if err != nil {
		return nil, err
	}
	e.wal = wal

	// Create Appender for every Raw.
	appenders := make(map[vanus.ID]Appender, len(storages))
	for id, r := range raws {
		s := storages[id]

		// Raft log has been compacted.
		if s == nil {
			log.Debug(ctx).
				Stringer("block_id", id).
				Msg("Raft storage of block has been deleted or not created, so skip it.")
			// TODO(james.yin): clean expired metadata
			continue
		}

		a := e.newAppender(r, s)

		appenders[id] = a
	}

	// Clean expired storages.
	for id, rs := range storages {
		if _, ok := appenders[id]; ok {
			continue
		}

		log.Debug(ctx).
			Stringer("block_id", id).
			Msg("Not found appender, so discard the raft storage.")

		rs.Delete(ctx)
	}

	return appenders, nil
}

func (e *Engine) Start() {
	e.executor.start()
}

func (e *Engine) Close(ctx context.Context) {
	// Close WAL, stateStore, hintStore.
	e.wal.Close()
	e.hintStore.Close()
	// Make sure WAL is closed before close stateStore.
	e.wal.Wait()
	e.stateStore.Close(ctx)

	// Close grpc connections for raft.
	e.host.Stop()

	// FIXME(james.yin): close all executors.

	close(e.closeC)
}

func (e *Engine) RegisterServer(srv *grpc.Server) {
	raftSrv := transport.NewServer(e.host)
	raftpb.RegisterRaftServerServer(srv, raftSrv)
}

func (e *Engine) NewAppender(ctx context.Context, raw block.Raw) (Appender, error) {
	s, err := storage.NewStorage(ctx, raw.ID(), e.wal, e.stateStore, e.hintStore, nil)
	if err != nil {
		return nil, err
	}
	return e.newAppender(raw, s), nil
}

func (e *Engine) newAppender(raw block.Raw, s *storage.Storage) Appender {
	a := &appender{
		raw:               raw,
		appendLis:         e.appendLis,
		leaderLis:         e.leaderLis,
		storage:           s,
		host:              e.host,
		e:                 e,
		hint:              make(map[uint64]string, 3),
		raftExecutor:      e.executor.newRaftFlow(),
		appendExecutor:    e.executor.newAppendFlow(),
		commitExecutor:    e.executor.newCommitFlow(),
		persistExecutor:   e.executor.newPersistFlow(),
		applyExecutor:     e.executor.newApplyFlow(),
		transportExecutor: e.executor.newTransportFlow(),
	}
	a.actx = a.raw.NewAppendContext(nil)

	a.storage.SetSnapshotOperator(a)
	a.host.Register(a.ID().Uint64(), a)

	s.AppendExecutor = a.persistExecutor
	c := &raft.Config{
		ID:                        a.ID().Uint64(),
		ElectionTick:              defaultElectionTick,
		HeartbeatTick:             defaultHeartbeatTick,
		Storage:                   a.storage,
		Keeper:                    a,
		Applied:                   s.Applied(),
		Compacted:                 s.Compacted(),
		MaxSizePerMsg:             defaultMaxSizePerMsg,
		MaxInflightMsgs:           defaultMaxInflightMsgs,
		PreVote:                   true,
		DisableProposalForwarding: true,
	}
	node, err := raft.NewRawNode(c)
	if err != nil {
		panic(err)
	}
	a.node = node

	e.mu.Lock()
	_ = e.appenders.PushBack(a)
	e.mu.Unlock()

	return a
}

func (e *Engine) runTick() {
	t := time.NewTicker(defaultTickInterval)
	defer t.Stop()

	for {
		select {
		case <-e.closeC:
			return
		case <-t.C:
			e.mu.RLock()
			for ee := e.appenders.Front(); ee != nil; {
				a, _ := ee.Value.(*appender)
				next := ee.Next()
				if !a.tick() {
					e.appenders.Remove(ee)
				}
				ee = next
			}
			e.mu.RUnlock()
		}
	}
}

func (e *Engine) RegisterNodeRecord(id uint64, val string) error {
	e.resolver.Register(id, val)
	return nil
}
