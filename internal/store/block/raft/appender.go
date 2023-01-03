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
		listener: listener,
		log:      raftLog,
		host:     host,
		hint:     make([]peer, 0, defaultHintCapacity),
		cancel:   cancel,
		doneC:    make(chan struct{}),
		tracer:   tracing.NewTracer("store.block.raft.appender", trace.SpanKindInternal),
	}
	a.actx = a.raw.NewAppendContext(nil)

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
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.Bootstrap() Start")
	defer span.AddEvent("store.block.raft.appender.Bootstrap() End")

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
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.Delete() Start")
	defer span.AddEvent("store.block.raft.appender.Delete() End")

	a.Stop(ctx)
	a.log.Delete(ctx)
}

func (a *appender) run(ctx context.Context) {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(defaultTickInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			a.node.Tick()
		case rd := <-a.node.Ready():
			rCtx, span := a.tracer.Start(ctx, "RaftReady", trace.WithNewRoot())

			if len(rd.Entries) != 0 {
				a.persistEntries(rCtx, rd.Entries)
			}

			if !raft.IsEmptyHardState(rd.HardState) {
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

			// TODO(james.yin): snapshot
			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = a.log.ApplySnapshot(rCtx, rd.Snapshot)
			}

			if len(rd.CommittedEntries) != 0 {
				a.applyEntries(rCtx, rd.CommittedEntries)
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

func (a *appender) persistEntries(ctx context.Context, entries []raftpb.Entry) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.persistEntries() Start")
	defer span.AddEvent("store.block.raft.appender.persistEntries() End")

	log.Debug(ctx, "Append entries to raft log.", map[string]interface{}{
		"node_id":        a.ID(),
		"appended_index": entries[0].Index,
		"entries_num":    len(entries),
	})

	a.log.Append(ctx, entries, func(re raftlog.AppendResult, err error) {
		if err != nil {
			if errors.Is(err, raftlog.ErrCompacted) || errors.Is(err, raftlog.ErrTruncated) {
				// FIXME(james.yin): report to raft?
				return
			}
			panic(err)
		}

		// Report entries has been persisted.
		_ = a.node.ReportLogged(ctx, re.Index, re.Term)
	})
}

func (a *appender) applyEntries(ctx context.Context, committedEntries []raftpb.Entry) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.applyEntries() Start")
	defer span.AddEvent("store.block.raft.appender.applyEntries() End")

	var cs *raftpb.ConfState
	for i := 0; i < len(committedEntries); i++ {
		pbEntry := &committedEntries[i]
		index := pbEntry.Index

		if pbEntry.Type == raftpb.EntryNormal {
			var frag block.Fragment
			if len(pbEntry.Data) != 0 {
				frag = block.NewFragment(pbEntry.Data)
			}
			// FIXME(james.yin): do not pass frag with nil value?
			a.raw.CommitAppend(ctx, frag, func() {
				log.Debug(ctx, "Store applied offset.", map[string]interface{}{
					"node_id":        a.ID(),
					"applied_offset": index,
				})
				a.onAppend(ctx, index)
			})
			continue
		}

		// Change membership.
		cs = a.applyConfChange(ctx, pbEntry)
		ch := make(chan struct{})
		go func() {
			if err := a.log.SetConfState(ctx, *cs); err != nil {
				panic(err)
			}
			close(ch)
		}()
		// FIXME(james.yin): do not pass frag with nil value?
		a.raw.CommitAppend(ctx, nil, func() {
			<-ch
			log.Debug(ctx, "Store applied offset for conf change.", map[string]interface{}{
				"node_id":        a.ID(),
				"applied_offset": index,
			})
			a.onAppend(ctx, index)
		})
	}
}

func (a *appender) onAppend(ctx context.Context, index uint64) {
	a.log.SetApplied(ctx, index)
	_ = a.node.ReportApplied(ctx, index)
}

func (a *appender) becomeLeader(ctx context.Context) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.becomeLeader() Start")
	defer span.AddEvent("store.block.raft.appender.becomeLeader() End")

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
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.reset() Start")
	defer span.AddEvent("store.block.raft.appender.reset() End")

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

// Append implements block.Appender.
func (a *appender) Append(ctx context.Context, entries []block.Entry, cb block.AppendCallback) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.block.raft.appender.Append() Start")
	defer span.AddEvent("store.block.raft.appender.Append() End")

	span.AddEvent("Acquiring append lock")
	a.appendMu.Lock()
	span.AddEvent("Got append lock")

	if !a.isLeader() {
		a.appendMu.Unlock()
		cb(nil, block.ErrNotLeader)
		return
	}

	if a.actx.Archived() {
		a.appendMu.Unlock()
		cb(nil, block.ErrFull)
		return
	}

	seqs, frag, enough, err := a.raw.PrepareAppend(ctx, a.actx, entries...)
	if err != nil {
		a.appendMu.Unlock()
		cb(nil, err)
		return
	}

	data, _ := block.MarshalFragment(ctx, frag)

	var pds []raft.ProposeData
	if enough {
		if frag, err := a.raw.PrepareArchive(ctx, a.actx); err == nil {
			archivedData, _ := block.MarshalFragment(ctx, frag)
			pds = make([]raft.ProposeData, 2)
			// FIXME(james.yin): revert archived if propose failed.
			pds[1] = raft.ProposeData{
				Data: archivedData,
			}
		} else {
			pds = make([]raft.ProposeData, 1)
		}
	} else {
		pds = make([]raft.ProposeData, 1)
	}

	pds[0] = raft.ProposeData{
		Data: data,
		Callback: func(err error) {
			if err != nil {
				cb(nil, err)
			} else {
				cb(seqs, nil)
			}
		},
	}

	a.node.Propose(ctx, pds...)

	a.appendMu.Unlock()
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
