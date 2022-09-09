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

//go:generate mockgen -source=appender.go  -destination=testing/mock_appender.go -package=testing
package raft

import (
	// standard libraries.
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	// third-party libraries.
	oteltracer "go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultHintCapacity    = 2
	defaultTickInterval    = 100 * time.Millisecond
	defaultElectionTick    = 10
	defaultHeartbeatTick   = 3
	defaultMaxSizePerMsg   = 4096
	defaultMaxInflightMsgs = 256
)

type Peer struct {
	ID       vanus.ID
	Endpoint string
}

type ClusterStatus struct {
	Leader vanus.ID
	Term   uint64
}

type peer struct {
	id       uint64
	endpoint string
}

type LeaderChangedListener func(block, leader vanus.ID, term uint64)

type commitWaiter struct {
	offset int64
	c      chan struct{}
}

type Appender interface {
	block.Appender

	Stop(ctx context.Context)
	Bootstrap(ctx context.Context, blocks []Peer) error
	Delete(ctx context.Context)
	Status() ClusterStatus
}

type appender struct {
	mu sync.RWMutex

	raw  block.Raw
	actx block.AppendContext

	waiters      []commitWaiter
	commitIndex  uint64
	commitOffset int64
	mu2          sync.Mutex

	leaderID vanus.ID
	listener LeaderChangedListener

	node raft.Node
	log  *raftlog.Log
	host transport.Host

	hint   []peer
	hintMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	donec  chan struct{}
	tracer *tracing.Tracer
}

// Make sure appender implements Appender.
var _ Appender = (*appender)(nil)

func NewAppender(
	ctx context.Context, raw block.Raw, raftLog *raftlog.Log, host transport.Host, listener LeaderChangedListener,
) Appender {
	ctx, cancel := context.WithCancel(ctx)

	r := &appender{
		raw:      raw,
		waiters:  make([]commitWaiter, 0),
		listener: listener,
		log:      raftLog,
		host:     host,
		hint:     make([]peer, 0, defaultHintCapacity),
		ctx:      ctx,
		cancel:   cancel,
		donec:    make(chan struct{}),
		tracer:   tracing.NewTracer("store.block.replica.replica", oteltracer.SpanKindInternal),
	}
	r.actx = r.raw.NewAppendContext(nil)
	r.commitOffset = r.actx.WriteOffset()

	r.log.SetSnapshotOperator(r)
	r.host.Register(r.ID().Uint64(), r)

	c := &raft.Config{
		ID:                        r.ID().Uint64(),
		ElectionTick:              defaultElectionTick,
		HeartbeatTick:             defaultHeartbeatTick,
		Storage:                   raftLog,
		Applied:                   raftLog.Applied(),
		Compacted:                 raftLog.Compacted(),
		MaxSizePerMsg:             defaultMaxSizePerMsg,
		MaxInflightMsgs:           defaultMaxInflightMsgs,
		PreVote:                   true,
		DisableProposalForwarding: true,
	}
	r.node = raft.RestartNode(c)

	// Access Commit after raft.RestartNode to ensure raft state is initialized.
	r.commitIndex = r.log.HardState().Commit

	go r.run(context.TODO())

	return r
}

func (r *appender) ID() vanus.ID {
	return r.raw.ID()
}

func (r *appender) Stop(ctx context.Context) {
	r.cancel()

	// Block until the stop has been acknowledged.
	<-r.donec

	log.Info(ctx, "the raft node stopped", map[string]interface{}{
		"node_id":   r.node,
		"leader_id": r.leaderID,
	})
}

func (r *appender) Bootstrap(ctx context.Context, blocks []Peer) error {
	_, span := r.tracer.Start(ctx, "Bootstrap")
	defer span.End()

	peers := make([]raft.Peer, 0, len(blocks))
	for _, ep := range blocks {
		peers = append(peers, raft.Peer{
			ID:      ep.ID.Uint64(),
			Context: []byte(ep.Endpoint),
		})
	}
	// sort peers
	sort.Slice(peers, func(a, b int) bool {
		return peers[a].ID < peers[b].ID
	})
	return r.node.Bootstrap(peers)
}

func (r *appender) Delete(ctx context.Context) {
	ctx, span := r.tracer.Start(ctx, "Delete")
	defer span.End()

	r.Stop(ctx)
	r.log.Delete(ctx)
}

func (r *appender) run(ctx context.Context) {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(defaultTickInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			ctx, span := r.tracer.Start(context.Background(), "RaftReady")
			var partial bool
			stateChanged := !raft.IsEmptyHardState(rd.HardState)
			if stateChanged {
				// Wake up fast before writing logs.
				partial = r.wakeup(rd.HardState.Commit) //nolint:contextcheck // wrong advice
			}

			if len(rd.Entries) != 0 {
				log.Debug(ctx, "Append entries to raft log.", map[string]interface{}{
					"block_id":       r.ID(),
					"appended_index": rd.Entries[0].Index,
					"entries_num":    len(rd.Entries),
				})
				if err := r.log.Append(ctx, rd.Entries); err != nil {
					span.End()
					panic(err)
				}
			}

			if stateChanged {
				// Wake up after writing logs.
				if partial {
					_ = r.wakeup(rd.HardState.Commit) //nolint:contextcheck // wrong advice
				}
				log.Debug(ctx, "Persist raft hard state.", map[string]interface{}{
					"block_id":   r.ID(),
					"hard_state": rd.HardState,
				})
				if err := r.log.SetHardState(ctx, rd.HardState); err != nil {
					span.End()
					panic(err)
				}
			}

			if rd.SoftState != nil {
				r.leaderID = vanus.NewIDFromUint64(rd.SoftState.Lead)
				if rd.SoftState.RaftState == raft.StateLeader {
					r.becomeLeader() //nolint:contextcheck // wrong advice
				}
			}

			// NOTE: Messages to be sent AFTER HardState and Entries are committed to stable storage.
			r.send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = r.log.ApplySnapshot(ctx, rd.Snapshot)
			}

			if len(rd.CommittedEntries) != 0 {
				applied := r.applyEntries(ctx, rd.CommittedEntries)
				log.Debug(ctx, "Store applied offset.", map[string]interface{}{
					"block_id":       r.ID(),
					"applied_offset": applied,
				})
				// FIXME(james.yin): persist applied after flush block.
				r.log.SetApplied(applied)
			}

			if rd.Compact != 0 {
				_ = r.log.Compact(ctx, rd.Compact)
			}

			r.node.Advance()
			span.End()
		case <-r.ctx.Done():
			r.node.Stop()
			close(r.donec)
			return
		}
	}
}

func (r *appender) applyEntries(ctx context.Context, committedEntries []raftpb.Entry) uint64 {
	ctx, span := r.tracer.Start(ctx, "applyEntries")
	defer span.End()

	num := len(committedEntries)
	if num == 0 {
		return 0
	}

	var cs *raftpb.ConfState
	frags := make([]block.Fragment, 0, num)
	for i := range committedEntries {
		entrypb := &committedEntries[i]

		if entrypb.Type == raftpb.EntryNormal {
			// Skip empty entry(raft heartbeat).
			if len(entrypb.Data) != 0 {
				frag := block.NewFragment(entrypb.Data)
				frags = append(frags, frag)
			}
			continue
		}

		// Change membership.
		cs = r.applyConfChange(entrypb)
	}

	if len(frags) != 0 {
		r.doAppend(ctx, frags...)
	}

	// ConfState is changed.
	if cs != nil {
		if err := r.log.SetConfState(ctx, *cs); err != nil {
			panic(err)
		}
	}

	return committedEntries[num-1].Index
}

// wakeup wakes up append requests to the smaller of the committed or last index.
func (r *appender) wakeup(commit uint64) (partial bool) {
	li, _ := r.log.LastIndex()
	if commit > li {
		commit = li
		partial = true
	}

	if commit <= r.commitIndex {
		return
	}
	r.commitIndex = commit

	for off := commit; off > 0; off-- {
		entrypbs, err := r.log.Entries(off, off+1, 0)
		if err != nil {
			return
		}

		entrypb := entrypbs[0]
		if entrypb.Type == raftpb.EntryNormal && len(entrypb.Data) > 0 {
			frag := block.NewFragment(entrypb.Data)
			r.doWakeup(frag.EndOffset())
			return
		}
	}

	return partial
}

func (r *appender) becomeLeader() {
	// Reset when become leader.
	r.reset()

	r.leaderChanged()
}

func (r *appender) leaderChanged() {
	if r.listener == nil {
		return
	}

	leader, term := r.leaderInfo()
	r.listener(r.ID(), leader, term)
}

func (r *appender) applyConfChange(entrypb *raftpb.Entry) *raftpb.ConfState {
	if entrypb.Type == raftpb.EntryNormal {
		// TODO(james.yin): return error
		return nil
	}

	var cci raftpb.ConfChangeI
	if entrypb.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entrypb.Data); err != nil {
			panic(err)
		}
		// TODO(james.yin): non-add
		r.hintPeer(cc.NodeID, string(cc.Context))
		cci = cc
	} else {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entrypb.Data); err != nil {
			panic(err)
		}
		// TODO(james.yin): non-add
		for _, ccs := range cc.Changes {
			r.hintPeer(ccs.NodeID, string(cc.Context))
		}
		cci = cc
	}
	return r.node.ApplyConfChange(cci)
}

func (r *appender) reset() {
	off, err := r.log.LastIndex()
	if err != nil {
		off = r.log.HardState().Commit
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for off > 0 {
		entrypbs, err2 := r.log.Entries(off, off+1, 0)

		// Entry has been compacted.
		if err2 != nil {
			r.actx = r.raw.NewAppendContext(nil)
			break
		}

		entrypb := entrypbs[0]
		if entrypb.Type == raftpb.EntryNormal && len(entrypb.Data) > 0 {
			frag := block.NewFragment(entrypb.Data)
			r.actx = r.raw.NewAppendContext(frag)
			break
		}

		off--
	}

	// no normal entry
	if off == 0 {
		r.actx = r.raw.NewAppendContext(nil)
	}
}

// Append implements block.raw.
func (r *appender) Append(ctx context.Context, entries ...block.Entry) ([]int64, error) {
	ctx, span := r.tracer.Start(ctx, "Append")
	defer span.End()

	seqs, offset, err := r.append(ctx, entries)
	if err != nil {
		if errors.Is(err, block.ErrFull) {
			_ = r.waitCommit(ctx, offset)
		}
		return nil, err
	}

	// Wait until entries is committed.
	err = r.waitCommit(ctx, offset)
	if err != nil {
		return nil, err
	}

	return seqs, nil
}

func (r *appender) append(ctx context.Context, entries []block.Entry) ([]int64, int64, error) {
	ctx, span := r.tracer.Start(ctx, "append")
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isLeader() {
		return nil, 0, block.ErrNotLeader
	}

	if r.actx.Archived() {
		return nil, r.actx.WriteOffset(), block.ErrFull
	}

	seqs, frag, enough, err := r.raw.PrepareAppend(ctx, r.actx, entries...)
	if err != nil {
		return nil, 0, err
	}
	off := r.actx.WriteOffset()

	data, _ := block.MarshalFragment(frag)
	if err = r.node.Propose(ctx, data); err != nil {
		return nil, 0, err
	}

	if enough {
		if frag, err = r.raw.PrepareArchive(ctx, r.actx); err == nil {
			data, _ := block.MarshalFragment(frag)
			_ = r.node.Propose(ctx, data)
			// FIXME(james.yin): revert archived if propose failed.
		}
	}

	return seqs, off, nil
}

func (r *appender) doAppend(ctx context.Context, frags ...block.Fragment) {
	if len(frags) == 0 {
		return
	}
	_, _ = r.raw.CommitAppend(ctx, frags...)
}

func (r *appender) waitCommit(ctx context.Context, offset int64) error {
	ctx, span := r.tracer.Start(ctx, "waitCommit")
	defer span.End()
	r.mu2.Lock()

	if offset <= r.commitOffset {
		r.mu2.Unlock()
		return nil
	}

	ch := make(chan struct{})
	r.waiters = append(r.waiters, commitWaiter{
		offset: offset,
		c:      ch,
	})

	r.mu2.Unlock()

	// FIXME(james.yin): lost leader
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *appender) doWakeup(commit int64) {
	r.mu2.Lock()
	defer r.mu2.Unlock()

	for len(r.waiters) != 0 {
		waiter := r.waiters[0]
		if waiter.offset > commit {
			break
		}
		close(waiter.c)
		r.waiters = r.waiters[1:]
	}
	r.commitOffset = commit
}

func (r *appender) Status() ClusterStatus {
	leader, term := r.leaderInfo()
	return ClusterStatus{
		Leader: leader,
		Term:   term,
	}
}

func (r *appender) leaderInfo() (vanus.ID, uint64) {
	// FIXME(james.yin): avoid concurrent issue.
	return r.leaderID, r.log.HardState().Term
}

func (r *appender) isLeader() bool {
	return r.leaderID == r.ID()
}
