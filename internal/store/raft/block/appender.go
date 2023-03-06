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

//go:generate mockgen -source=appender.go -destination=testing/mock_appender.go -package=testing
package block

import (
	// standard libraries.
	"context"
	"errors"
	"sort"
	"time"

	// third-party libraries.

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/raft"
	"github.com/vanus-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/executor"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/raft/storage"
	"github.com/linkall-labs/vanus/internal/store/raft/transport"
)

const (
	defaultHintCapacity    = 2
	defaultTickInterval    = 100 * time.Millisecond
	defaultElectionTick    = 10
	defaultHeartbeatTick   = 3
	defaultMaxSizePerMsg   = 16 * 1024
	defaultMaxInflightMsgs = 256
	defaultSendTimeout     = 80 * time.Millisecond
)

type Peer struct {
	ID       vanus.ID
	Endpoint string
}

type ClusterStatus struct {
	Leader vanus.ID
	Term   uint64
}

type LeaderChangedListener = func(block, leader vanus.ID, term uint64)

type EntryAppendedListener = func(block vanus.ID)

type Appender interface {
	block.Appender

	Stop(ctx context.Context)
	Delete(ctx context.Context)
	Bootstrap(ctx context.Context, blocks []Peer) error
	Status() ClusterStatus
}

type appender struct {
	raw       block.Raw
	actx      block.AppendContext
	appendLis EntryAppendedListener

	leaderID  vanus.ID
	leaderLis LeaderChangedListener

	node    *raft.RawNode
	storage *storage.Storage
	host    transport.Host

	hint map[uint64]string

	raftExecutor      executor.ExecuteCloser
	appendExecutor    executor.ExecuteCloser
	commitExecutor    executor.ExecuteCloser
	persistExecutor   executor.ExecuteCloser
	applyExecutor     executor.ExecuteCloser
	transportExecutor executor.ExecuteCloser
}

// Make sure appender implements Appender.
var _ Appender = (*appender)(nil)

func (a *appender) ID() vanus.ID {
	return a.raw.ID()
}

func (a *appender) Stop(ctx context.Context) {
	// TODO(james.yin): waiting for acknowledgments from executors is unnecessary?
	a.transportExecutor.Close()
	a.raftExecutor.Close()
	a.applyExecutor.Close()
	a.persistExecutor.Close()
	a.commitExecutor.Close()

	log.Info(ctx, "raft appender is stopped.", map[string]interface{}{
		"node_id":   a.ID(),
		"leader_id": a.leaderID,
	})
}

func (a *appender) Delete(ctx context.Context) {
	a.Stop(ctx)
	a.storage.Delete(ctx)

	// FIXME(james.yin): wakeup inflight append calls?

	log.Info(ctx, "raft appender is deleted.", map[string]interface{}{
		"node_id": a.ID(),
	})
}

func (a *appender) Bootstrap(ctx context.Context, blocks []Peer) error {
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
	return a.bootstrap(peers)
}

func (a *appender) persistHardState(ctx context.Context, hs raftpb.HardState) {
	a.storage.SetHardState(ctx, hs, func(err error) {
		if err != nil {
			panic(err)
		}
		a.reportStateStatus(ctx, hs.Term, hs.Vote)
	})
}

func (a *appender) persistEntries(ctx context.Context, entries []raftpb.Entry) {
	// log.Debug(ctx, "Append entries to raft log.", map[string]interface{}{
	// 	"node_id":        a.ID(),
	// 	"appended_index": entries[0].Index,
	// 	"entries_num":    len(entries),
	// })

	a.storage.Append(ctx, entries, func(re storage.AppendResult, err error) {
		if err != nil {
			if errors.Is(err, storage.ErrCompacted) || errors.Is(err, storage.ErrTruncated) {
				// FIXME(james.yin): report to raft?
				return
			}
			panic(err)
		}

		// Report entries has been persisted.
		a.reportLogStatus(ctx, re.Index, re.Term)
	})
}

func (a *appender) compactLog(ctx context.Context, index uint64) {
	// log.Debug(ctx, "Compact raft log.", map[string]interface{}{
	// 	"node_id": a.ID(),
	// 	"index":   index,
	// })

	_ = a.storage.Compact(ctx, index)
}

func (a *appender) applyEntries(ctx context.Context, committedEntries []raftpb.Entry) {
	for i := 0; i < len(committedEntries); i++ {
		pbEntry := &committedEntries[i]

		// Change membership.
		if pbEntry.Type != raftpb.EntryNormal {
			a.changeMembership(ctx, pbEntry)
			continue
		}

		var frag block.Fragment
		if len(pbEntry.Data) != 0 {
			frag = block.NewFragment(pbEntry.Data)
		}

		index := pbEntry.Index
		// FIXME(james.yin): do not pass frag with nil value?
		a.raw.CommitAppend(ctx, frag, func() {
			// log.Debug(ctx, "Store applied offset.", map[string]interface{}{
			// 	"node_id":        a.ID(),
			// 	"applied_offset": index,
			// })
			a.onAppend(ctx, index)

			if frag != nil && a.appendLis != nil {
				a.appendLis(a.ID())
			}
		})
	}
}

func (a *appender) onAppend(ctx context.Context, index uint64) {
	a.storage.SetApplied(ctx, index)
	a.reportApplyStatus(ctx, index)
}

func (a *appender) changeMembership(ctx context.Context, pbEntry *raftpb.Entry) {
	cs := a.changeConf(ctx, pbEntry)

	ch := make(chan struct{})
	a.storage.SetConfState(ctx, *cs, func(err error) {
		if err != nil {
			panic(err)
		}
		close(ch)
	})

	index := pbEntry.Index
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

func (a *appender) changeConf(ctx context.Context, pbEntry *raftpb.Entry) *raftpb.ConfState {
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
		a.transportExecutor.Execute(func() {
			if cc.Type == raftpb.ConfChangeRemoveNode {
				delete(a.hint, cc.NodeID)
			} else {
				a.hint[cc.NodeID] = string(cc.Context)
			}
		})
		cci = cc
	} else {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(pbEntry.Data); err != nil {
			panic(err)
		}
		changes := cc.Changes
		a.transportExecutor.Execute(func() {
			// FIXME(james.yin): check it.
			for _, ccs := range changes {
				if ccs.Type == raftpb.ConfChangeRemoveNode {
					delete(a.hint, ccs.NodeID)
				} else {
					a.hint[ccs.NodeID] = string(cc.Context)
				}
			}
		})
		cci = cc
	}
	return a.applyConfChange(cci)
}

func (a *appender) becomeLeader(ctx context.Context) {
	log.Info(ctx, "Block become leader.", map[string]interface{}{
		"node_id": a.ID(),
	})

	// Reset append context when become leader.
	a.resetAppendContext()

	a.onLeaderChanged()
}

func (a *appender) onLeaderChanged() {
	if a.leaderLis == nil {
		return
	}

	leader, term := a.leaderInfo()
	a.leaderLis(a.ID(), leader, term)
}

func (a *appender) resetAppendContext() {
	ch := make(chan struct{})
	a.appendExecutor.Execute(func() {
		a.doReset()
		close(ch)
	})
	<-ch
}

func (a *appender) doReset() {
	off, err := a.storage.LastIndex()
	if err != nil {
		off = a.storage.Commit() // unreachable
	}

	for off > 0 {
		pbEntries, err2 := a.storage.Entries(off, off+1, 0)

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
	a.appendExecutor.Execute(func() {
		a.doAppend(ctx, entries, cb)
	})
}

func (a *appender) doAppend(ctx context.Context, entries []block.Entry, cb block.AppendCallback) {
	if !a.isLeader() {
		cb(nil, block.ErrNotLeader)
		return
	}

	if a.actx.Archived() {
		cb(nil, block.ErrFull)
		return
	}

	seqs, frag, enough, err := a.raw.PrepareAppend(ctx, a.actx, entries...)
	if err != nil {
		cb(nil, err)
		return
	}

	data, _ := block.MarshalFragment(frag)

	var pds []raft.ProposeData
	if enough {
		if frag, err := a.raw.PrepareArchive(ctx, a.actx); err == nil {
			archivedData, _ := block.MarshalFragment(frag)
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

	a.propose(pds...)
}

func (a *appender) Status() ClusterStatus {
	leader, term := a.leaderInfo()
	return ClusterStatus{
		Leader: leader,
		Term:   term,
	}
}

func (a *appender) leaderInfo() (vanus.ID, uint64) {
	return a.leaderID, a.storage.HardState().Term
}

func (a *appender) isLeader() bool {
	return a.leaderID == a.ID()
}
