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
	stderr "errors"
	"sort"
	"sync"
	"time"

	// third-party libraries.
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/pkg/errors"
)

const (
	defaultHintCapacity    = 2
	defaultTickInterval    = 100 * time.Millisecond
	defaultElectionTick    = 10
	defaultHeartbeatTick   = 3
	defaultMaxSizePerMsg   = 4096
	defaultMaxInflightMsgs = 256
	defaultWaiterWorker    = 8
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
	seqs     []int64
	offset   int64
	err      error
	callback func([]int64, error)
}

type Appender interface {
	block.Appender

	Stop(ctx context.Context)
	Bootstrap(ctx context.Context, blocks []Peer) error
	Delete(ctx context.Context)
	Status() ClusterStatus
}

type appender struct {
	raw      block.Raw
	actx     block.AppendContext
	appendMu sync.RWMutex

	waiters      []commitWaiter
	waiterC      chan commitWaiter
	commitIndex  uint64
	commitOffset int64
	waitMu       sync.Mutex

	leaderID vanus.ID
	listener LeaderChangedListener

	node raft.Node
	log  *raftlog.Log
	host transport.Host

	hint   []peer
	hintMu sync.RWMutex

	cancel context.CancelFunc
	doneC  chan struct{}
	tracer *tracing.Tracer
}

// Make sure appender implements Appender.
var _ Appender = (*appender)(nil)

func NewAppender(
	ctx context.Context, raw block.Raw, raftLog *raftlog.Log, host transport.Host, listener LeaderChangedListener,
) Appender {
	ctx, cancel := context.WithCancel(ctx)

	a := &appender{
		raw:      raw,
		waiters:  make([]commitWaiter, 0),
		waiterC:  make(chan commitWaiter),
		listener: listener,
		log:      raftLog,
		host:     host,
		hint:     make([]peer, 0, defaultHintCapacity),
		cancel:   cancel,
		doneC:    make(chan struct{}),
		tracer:   tracing.NewTracer("store.block.raft.appender", trace.SpanKindInternal),
	}
	a.actx = a.raw.NewAppendContext(nil)
	a.commitOffset = a.actx.WriteOffset()

	a.log.SetSnapshotOperator(a)
	a.host.Register(a.ID().Uint64(), a)

	c := &raft.Config{
		ID:                        a.ID().Uint64(),
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
	a.node = raft.RestartNode(c)

	// Access Commit after raft.RestartNode to ensure raft state is initialized.
	a.commitIndex = a.log.HardState().Commit

	go a.run(ctx)

	return a
}

func (a *appender) ID() vanus.ID {
	return a.raw.ID()
}

func (a *appender) Stop(ctx context.Context) {
	a.cancel()

	// Block until the stop has been acknowledged.
	<-a.doneC

	log.Info(ctx, "the raft node stopped", map[string]interface{}{
		"node_id":   a.ID(),
		"leader_id": a.leaderID,
	})
}

func (a *appender) Bootstrap(ctx context.Context, blocks []Peer) error {
	_, span := a.tracer.Start(ctx, "Bootstrap")
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
	return a.node.Bootstrap(peers)
}

func (a *appender) Delete(ctx context.Context) {
	ctx, span := a.tracer.Start(ctx, "Delete")
	defer span.End()

	a.Stop(ctx)
	a.log.Delete(ctx)
}

func (a *appender) runWaiterWorker(ctx context.Context) {
	for i := 0; i < defaultWaiterWorker; i++ {
		go func() {
			for {
				select {
				case waiter := <-a.waiterC:
					waiter.callback(waiter.seqs, waiter.err)
				case <-ctx.Done():
					close(a.waiterC)
					return
				}
			}
		}()
	}
}

func (a *appender) run(ctx context.Context) {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(defaultTickInterval)
	defer t.Stop()

	a.runWaiterWorker(ctx)

	for {
		select {
		case <-t.C:
			a.node.Tick()
		case rd := <-a.node.Ready():
			rCtx, span := a.tracer.Start(ctx, "RaftReady", trace.WithNewRoot())

			var partial bool
			stateChanged := !raft.IsEmptyHardState(rd.HardState)
			if stateChanged {
				// Wake up fast before writing logs.
				partial = a.wakeup(rCtx, rd.HardState.Commit)
			}

			if len(rd.Entries) != 0 {
				log.Debug(rCtx, "Append entries to raft log.", map[string]interface{}{
					"node_id":        a.ID(),
					"appended_index": rd.Entries[0].Index,
					"entries_num":    len(rd.Entries),
				})
				a.log.Append(rCtx, rd.Entries, func(re raftlog.AppendResult, err error) {
					if err != nil {
						if stderr.Is(err, raftlog.ErrCompacted) || stderr.Is(err, raftlog.ErrTruncated) {
							// FIXME(james.yin): report to raft?
							return
						}
						panic(err)
					}

					// Report entries has been persisted.
					_ = a.node.Step(ctx, raftpb.Message{
						Type:    raftpb.MsgLogResp,
						LogTerm: re.Term,
						Index:   re.Index,
					})
				})
			}

			if stateChanged {
				// Wake up after writing logs.
				if partial {
					_ = a.wakeup(rCtx, rd.HardState.Commit)
				}
				log.Debug(rCtx, "Persist raft hard state.", map[string]interface{}{
					"node_id":    a.ID(),
					"hard_state": rd.HardState,
				})
				if err := a.log.SetHardState(rCtx, rd.HardState); err != nil {
					span.End()
					panic(err)
				}
			}

			if rd.SoftState != nil {
				a.leaderID = vanus.NewIDFromUint64(rd.SoftState.Lead)
				if rd.SoftState.RaftState == raft.StateLeader {
					a.becomeLeader(rCtx)
				}
			}

			// NOTE: Messages to be sent AFTER HardState and Entries are committed to stable storage.
			a.send(rCtx, rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = a.log.ApplySnapshot(rCtx, rd.Snapshot)
			}

			if len(rd.CommittedEntries) != 0 {
				applied := a.applyEntries(rCtx, rd.CommittedEntries)
				log.Debug(rCtx, "Store applied offset.", map[string]interface{}{
					"node_id":        a.ID(),
					"applied_offset": applied,
				})
				// FIXME(james.yin): persist applied after flush block.
				a.log.SetApplied(rCtx, applied)
			}

			// TODO(james.yin): optimize
			if rd.Compact != 0 {
				_ = a.log.Compact(rCtx, rd.Compact)
			}

			_, span2 := a.tracer.Start(rCtx, "Advance")
			a.node.Advance()
			span2.End()

			span.End()
		case <-ctx.Done():
			a.node.Stop()
			close(a.doneC)
			return
		}
	}
}

func (a *appender) applyEntries(ctx context.Context, committedEntries []raftpb.Entry) uint64 {
	ctx, span := a.tracer.Start(ctx, "applyEntries")
	defer span.End()

	num := len(committedEntries)
	if num == 0 {
		return 0
	}

	var cs *raftpb.ConfState
	frags := make([]block.Fragment, 0, num)
	for i := range committedEntries {
		pbEntry := &committedEntries[i]

		if pbEntry.Type == raftpb.EntryNormal {
			// Skip empty entry(raft heartbeat).
			if len(pbEntry.Data) != 0 {
				frag := block.NewFragment(pbEntry.Data)
				frags = append(frags, frag)
			}
			continue
		}

		// Change membership.
		cs = a.applyConfChange(ctx, pbEntry)
	}

	if len(frags) != 0 {
		a.doAppend(ctx, frags...)
	}

	// ConfState is changed.
	if cs != nil {
		if err := a.log.SetConfState(ctx, *cs); err != nil {
			panic(err)
		}
	}

	return committedEntries[num-1].Index
}

// wakeup wakes up append requests to the smaller of the committed or last index.
func (a *appender) wakeup(ctx context.Context, commit uint64) (partial bool) {
	_, span := a.tracer.Start(ctx, "wakeup", trace.WithAttributes(
		attribute.Int64("commit", int64(commit))))
	defer span.End()

	li, _ := a.log.LastIndex()
	if commit > li {
		commit = li
		partial = true
	}

	if commit <= a.commitIndex {
		return
	}
	a.commitIndex = commit

	for off := commit; off > 0; off-- {
		pbEntries, err := a.log.Entries(off, off+1, 0)
		if err != nil {
			return
		}

		pbEntry := pbEntries[0]
		if pbEntry.Type == raftpb.EntryNormal && len(pbEntry.Data) > 0 {
			frag := block.NewFragment(pbEntry.Data)
			a.doWakeup(ctx, frag.EndOffset())
			return
		}
	}

	return partial
}

func (a *appender) becomeLeader(ctx context.Context) {
	ctx, span := a.tracer.Start(ctx, "becomeLeader")
	defer span.End()

	// Reset when become leader.
	a.reset(ctx)

	a.leaderChanged()
}

func (a *appender) leaderChanged() {
	if a.listener == nil {
		return
	}

	leader, term := a.leaderInfo()
	a.listener(a.ID(), leader, term)
}

func (a *appender) applyConfChange(ctx context.Context, pbEntry *raftpb.Entry) *raftpb.ConfState {
	if pbEntry.Type == raftpb.EntryNormal {
		// TODO(james.yin): return error
		return nil
	}

	var cci raftpb.ConfChangeI
	if pbEntry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(pbEntry.Data); err != nil {
			panic(err)
		}
		// TODO(james.yin): non-add
		a.hintPeer(ctx, cc.NodeID, string(cc.Context))
		cci = cc
	} else {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(pbEntry.Data); err != nil {
			panic(err)
		}
		// TODO(james.yin): non-add
		for _, ccs := range cc.Changes {
			a.hintPeer(ctx, ccs.NodeID, string(cc.Context))
		}
		cci = cc
	}
	return a.node.ApplyConfChange(cci)
}

func (a *appender) reset(ctx context.Context) {
	_, span := a.tracer.Start(ctx, "reset")
	defer span.End()

	off, err := a.log.LastIndex()
	if err != nil {
		off = a.log.HardState().Commit
	}

	span.AddEvent("Acquiring append lock.")
	a.appendMu.Lock()
	span.AddEvent("Got append lock.")

	defer a.appendMu.Unlock()

	for off > 0 {
		pbEntries, err2 := a.log.Entries(off, off+1, 0)

		// Entry has been compacted.
		if err2 != nil {
			a.actx = a.raw.NewAppendContext(nil)
			break
		}

		pbEntry := pbEntries[0]
		if pbEntry.Type == raftpb.EntryNormal && len(pbEntry.Data) > 0 {
			frag := block.NewFragment(pbEntry.Data)
			a.actx = a.raw.NewAppendContext(frag)
			break
		}

		off--
	}

	// no normal entry
	if off == 0 {
		a.actx = a.raw.NewAppendContext(nil)
	}
}

// Append implements async block.raw.
func (a *appender) Append(ctx context.Context, cb func([]int64, error), entries ...block.Entry) {
	ctx, span := a.tracer.Start(ctx, "Append")
	defer span.End()

	seqs, offset, err := a.append(ctx, entries)
	if err != nil && !errors.Is(err, errors.ErrFull) {
		cb(nil, err)
		return
	}

	// register callback and wait until entries is committed.
	a.registerCommitWaiter(ctx, commitWaiter{
		seqs:     seqs,
		offset:   offset,
		err:      err,
		callback: cb,
	})
}

func (a *appender) append(ctx context.Context, entries []block.Entry) ([]int64, int64, error) {
	ctx, span := a.tracer.Start(ctx, "append")
	defer span.End()

	span.AddEvent("Acquiring append lock")
	a.appendMu.Lock()
	span.AddEvent("Got append lock")

	defer a.appendMu.Unlock()

	if !a.isLeader() {
		return nil, 0, errors.ErrNotLeader.WithMessage("the appender is not leader")
	}

	if a.actx.Archived() {
		return nil, a.actx.WriteOffset(), errors.ErrFull.WithMessage("block is full")
	}

	seqs, frag, enough, err := a.raw.PrepareAppend(ctx, a.actx, entries...)
	if err != nil {
		return nil, 0, err
	}
	off := a.actx.WriteOffset()

	data, _ := block.MarshalFragment(ctx, frag)
	if err = a.node.Propose(ctx, data); err != nil {
		return nil, 0, err
	}

	if enough {
		if frag, err = a.raw.PrepareArchive(ctx, a.actx); err == nil {
			data, _ := block.MarshalFragment(ctx, frag)
			_ = a.node.Propose(ctx, data)
			// FIXME(james.yin): revert archived if propose failed.
		}
	}

	return seqs, off, nil
}

func (a *appender) doAppend(ctx context.Context, frags ...block.Fragment) {
	if len(frags) == 0 {
		return
	}
	_, _ = a.raw.CommitAppend(ctx, frags...)
}

func (a *appender) registerCommitWaiter(ctx context.Context, waiter commitWaiter) {
	_, span := a.tracer.Start(ctx, "waitCommit")
	defer span.End()

	span.AddEvent("Acquiring wait lock")
	a.waitMu.Lock()
	defer a.waitMu.Unlock()
	span.AddEvent("Got wait lock")

	if waiter.offset <= a.commitOffset {
		waiter.callback(waiter.seqs, waiter.err)
		return
	}
	a.waiters = append(a.waiters, waiter)
}

func (a *appender) doWakeup(ctx context.Context, commit int64) {
	_, span := a.tracer.Start(ctx, "doWakeup")
	defer span.End()

	span.AddEvent("Acquiring wait lock")
	a.waitMu.Lock()
	span.AddEvent("Got wait lock")

	defer a.waitMu.Unlock()

	for len(a.waiters) != 0 {
		waiter := a.waiters[0]
		if waiter.offset > commit {
			break
		}
		a.waiterC <- waiter
		a.waiters = a.waiters[1:]
	}
	a.commitOffset = commit
}

func (a *appender) Status() ClusterStatus {
	leader, term := a.leaderInfo()
	return ClusterStatus{
		Leader: leader,
		Term:   term,
	}
}

func (a *appender) leaderInfo() (vanus.ID, uint64) {
	// FIXME(james.yin): avoid concurrent issue.
	return a.leaderID, a.log.HardState().Term
}

func (a *appender) isLeader() bool {
	return a.leaderID == a.ID()
}
