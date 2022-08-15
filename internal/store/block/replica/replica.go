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

//go:generate mockgen -source=replica.go  -destination=mock_replica.go -package=replica
package replica

import (
	// standard libraries.
	"context"
	stderr "errors"
	"sort"
	"sync"
	"time"

	// first-party libraries.
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/block/file"
	"github.com/linkall-labs/vanus/internal/store/errors"
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

type peer struct {
	id       uint64
	endpoint string
}

type LeaderChangedListener func(block, leader vanus.ID, term uint64)

type commitWaiter struct {
	offset uint32
	c      chan struct{}
}

type Replica interface {
	Stop(ctx context.Context)
	Bootstrap(blocks []Peer) error
	Append(ctx context.Context, entries ...block.Entry) error
	Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string)
	FillClusterInfo(info *metapb.SegmentHealthInfo)
	Delete(ctx context.Context)
}

type replica struct {
	blockID vanus.ID

	mu sync.RWMutex

	appender block.TwoPCAppender
	actx     block.AppendContext

	waiters      []commitWaiter
	commitIndex  uint64
	commitOffset uint32
	mu2          sync.Mutex

	leaderID vanus.ID
	listener LeaderChangedListener

	node   raft.Node
	log    *raftlog.Log
	sender transport.Sender

	hint   []peer
	hintMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	donec  chan struct{}
}

// Make sure replica implements block.Appender, block.ClusterInfoSource and transport.Receiver.
var (
	_ block.Appender          = (*replica)(nil)
	_ block.ClusterInfoSource = (*replica)(nil)
	_ transport.Receiver      = (*replica)(nil)
)

func New(ctx context.Context, blockID vanus.ID, appender block.TwoPCAppender, raftLog *raftlog.Log,
	sender transport.Sender, listener LeaderChangedListener,
) Replica {
	ctx, cancel := context.WithCancel(ctx)

	r := &replica{
		blockID:  blockID,
		appender: appender,
		waiters:  make([]commitWaiter, 0),
		listener: listener,
		log:      raftLog,
		sender:   sender,
		hint:     make([]peer, 0, defaultHintCapacity),
		ctx:      ctx,
		cancel:   cancel,
		donec:    make(chan struct{}),
	}
	r.actx = r.appender.NewAppendContext(nil)
	r.commitOffset = r.actx.WriteOffset()

	c := &raft.Config{
		ID:                        blockID.Uint64(),
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
	r.commitIndex = r.log.HardState().Commit

	go r.run(context.TODO())

	return r
}

func (r *replica) IDStr() string {
	return r.appender.(*file.Block).IDStr()
}

func (r *replica) Stop(ctx context.Context) {
	r.cancel()
	// Block until the stop has been acknowledged.
	<-r.donec
	log.Info(ctx, "the raft node stopped", map[string]interface{}{
		"node_id":   r.node,
		"leader_id": r.leaderID,
	})
}

func (r *replica) Bootstrap(blocks []Peer) error {
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

func (r *replica) Delete(ctx context.Context) {
	r.Stop(ctx)
	r.log.Destroy(ctx)
}

func (r *replica) run(ctx context.Context) {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(defaultTickInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			var partial bool
			stateChanged := !raft.IsEmptyHardState(rd.HardState)
			if stateChanged {
				partial = r.wakeup(rd.HardState.Commit)
			}

			if len(rd.Entries) != 0 {
				log.Debug(ctx, "Append entries to raft log.", map[string]interface{}{
					"block_id":       r.blockID,
					"appended_index": rd.Entries[0].Index,
					"entries_num":    len(rd.Entries),
				})
				if err := r.log.Append(rd.Entries); err != nil {
					panic(err)
				}
			}

			if stateChanged {
				if partial {
					_ = r.wakeup(rd.HardState.Commit)
				}
				log.Debug(ctx, "Persist raft hard state.", map[string]interface{}{
					"block_id":   r.blockID,
					"hard_state": rd.HardState,
				})
				if err := r.log.SetHardState(rd.HardState); err != nil {
					panic(err)
				}
			}

			if rd.SoftState != nil {
				r.leaderID = vanus.NewIDFromUint64(rd.SoftState.Lead)
				if rd.SoftState.RaftState == raft.StateLeader {
					r.becomeLeader()
				}
			}

			// NOTE: Messages to be sent AFTER HardState and Entries
			// are committed to stable storage.
			r.send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = r.log.ApplySnapshot(rd.Snapshot)
			}

			if len(rd.CommittedEntries) != 0 {
				applied := r.applyEntries(ctx, rd.CommittedEntries)
				log.Debug(ctx, "Store applied offset.", map[string]interface{}{
					"block_id":       r.blockID,
					"applied_offset": applied,
				})
				// FIXME(james.yin): persist applied after flush block.
				r.log.SetApplied(applied)
			}

			if rd.Compact != 0 {
				_ = r.log.Compact(rd.Compact)
			}

			r.node.Advance()
		case <-r.ctx.Done():
			r.node.Stop()
			close(r.donec)
			return
		}
	}
}

func (r *replica) applyEntries(ctx context.Context, committedEntries []raftpb.Entry) uint64 {
	num := len(committedEntries)
	if num == 0 {
		return 0
	}

	var cs *raftpb.ConfState
	entries := make([]block.Entry, 0, num)
	for i := range committedEntries {
		entrypb := &committedEntries[i]

		if entrypb.Type == raftpb.EntryNormal {
			// Skip empty entry(raft heartbeat).
			if len(entrypb.Data) != 0 {
				entry := block.UnmarshalEntryWithOffsetAndIndex(entrypb.Data)
				entries = append(entries, entry)
			}
			continue
		}

		// Change membership.
		cs = r.applyConfChange(entrypb)
	}

	if len(entries) != 0 {
		r.doAppend(ctx, entries...)
	}

	// ConfState is changed.
	if cs != nil {
		if err := r.log.SetConfState(*cs); err != nil {
			panic(err)
		}
	}

	return committedEntries[num-1].Index
}

func (r *replica) wakeup(commit uint64) (partial bool) {
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
			offset := block.EntryEndOffset(entrypb.Data)
			r.doWakeup(offset)
			return
		}
	}

	return partial
}

func (r *replica) becomeLeader() {
	// Reset when become leader.
	r.reset()

	r.leaderChanged()
}

func (r *replica) leaderChanged() {
	if r.listener == nil {
		return
	}

	leader, term := r.leaderInfo()
	r.listener(r.blockID, leader, term)
}

func (r *replica) applyConfChange(entrypb *raftpb.Entry) *raftpb.ConfState {
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

func (r *replica) reset() {
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
			r.actx = r.appender.NewAppendContext(nil)
			break
		}

		entrypb := entrypbs[0]
		if entrypb.Type == raftpb.EntryNormal && len(entrypb.Data) > 0 {
			entry := block.UnmarshalEntryWithOffsetAndIndex(entrypb.Data)
			r.actx = r.appender.NewAppendContext(&entry)
			break
		}

		off--
	}

	// no normal entry
	if off == 0 {
		r.actx = r.appender.NewAppendContext(nil)
	}
}

// Append implements block.Appender.
func (r *replica) Append(ctx context.Context, entries ...block.Entry) error {
	// TODO(james.yin): support batch
	if len(entries) != 1 {
		return errors.ErrInvalidRequest
	}

	offset, err := r.append(ctx, entries)
	if err != nil {
		return err
	}

	// Wait until entries is committed.
	r.waitCommit(ctx, offset)

	return nil
}

func (r *replica) append(ctx context.Context, entries []block.Entry) (uint32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.isLeader() {
		return 0, block.ErrNotLeader
	}

	if r.actx.Full() {
		return 0, block.ErrFull
	}

	entries, err := r.appender.PrepareAppend(ctx, r.actx, entries...)
	if err != nil {
		// Mark as full, if there is not enough space.
		// TODO(james.yin): improve space utilization.
		if stderr.Is(err, block.ErrNotEnoughSpace) {
			entry := r.actx.FullEntry()
			data := entry.MarshalWithOffsetAndIndex()
			if err2 := r.node.Propose(ctx, data); err2 != nil {
				return 0, err2
			}
			r.actx.MarkFull()
		}
		return 0, err
	}

	// FIXME(james.yin): batch propose
	data := entries[0].MarshalWithOffsetAndIndex()
	if err := r.node.Propose(ctx, data); err != nil {
		return 0, err
	}

	return r.actx.WriteOffset(), nil
}

func (r *replica) doAppend(_ context.Context, entries ...block.Entry) {
	num := len(entries)
	if num == 0 {
		return
	}

	last := &entries[num-1]
	if len(last.Payload) == 0 {
		entries = entries[:num-1]
	} else {
		last = nil
	}

	if len(entries) != 0 {
		// FIXME(james.yin): context
		_ = r.appender.CommitAppend(context.TODO(), entries...)
	}

	// Mark full.
	if last != nil {
		_ = r.appender.MarkFull(context.TODO())
	}
}

func (r *replica) waitCommit(ctx context.Context, offset uint32) {
	r.mu2.Lock()

	if offset <= r.commitOffset {
		r.mu2.Unlock()
		return
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
	case <-ctx.Done():
	}
}

func (r *replica) doWakeup(commit uint32) {
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

func (r *replica) send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if len(msgs) == 1 {
		msg := &msgs[0]
		endpoint := r.peerHint(msg.To)
		r.sender.Send(r.ctx, msg, msg.To, endpoint, func(err error) {
			if err != nil {
				r.node.ReportUnreachable(msg.To)
			}
		})
		return
	}

	to := msgs[0].To
	for i := 1; i < len(msgs); i++ {
		if msgs[i].To != to {
			to = 0
			break
		}
	}

	// send to same node
	if to != 0 {
		endpoint := r.peerHint(to)
		for i := 0; i < len(msgs); i++ {
			r.sender.Send(r.ctx, &msgs[i], to, endpoint, func(err error) {
				if err != nil {
					r.node.ReportUnreachable(to)
				}
			})
		}
		return
	}

	mm := make(map[uint64][]*raftpb.Message)
	for i := 0; i < len(msgs); i++ {
		msg := &msgs[i]
		mm[msg.To] = append(mm[msg.To], msg)
	}
	for to, msgs := range mm {
		endpoint := r.peerHint(to)
		if len(msgs) == 1 {
			r.sender.Send(r.ctx, msgs[0], to, endpoint, func(err error) {
				if err != nil {
					r.node.ReportUnreachable(to)
				}
			})
		} else {
			for _, m := range msgs {
				r.sender.Send(r.ctx, m, to, endpoint, func(err error) {
					if err != nil {
						r.node.ReportUnreachable(to)
					}
				})
			}
		}
	}
}

func (r *replica) peerHint(to uint64) string {
	r.hintMu.RLock()
	defer r.hintMu.RUnlock()
	for i := range r.hint {
		p := &r.hint[i]
		if p.id == to {
			return p.endpoint
		}
	}
	return ""
}

// Receive implements transport.Receiver.
func (r *replica) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	if endpoint != "" {
		r.hintPeer(from, endpoint)
	}

	// TODO(james.yin): check ctx.Done().
	_ = r.node.Step(r.ctx, *msg)
}

func (r *replica) hintPeer(from uint64, endpoint string) {
	if endpoint == "" {
		return
	}

	// TODO(james.yin): optimize lock
	r.hintMu.Lock()
	defer r.hintMu.Unlock()
	p := func() *peer {
		for i := range r.hint {
			ep := &r.hint[i]
			if ep.id == from {
				return ep
			}
		}
		return nil
	}()
	if p == nil {
		r.hint = append(r.hint, peer{
			id:       from,
			endpoint: endpoint,
		})
	} else if p.endpoint != endpoint {
		p.endpoint = endpoint
	}
}

func (r *replica) FillClusterInfo(info *metapb.SegmentHealthInfo) {
	leader, term := r.leaderInfo()
	info.Leader = leader.Uint64()
	info.Term = term
}

func (r *replica) leaderInfo() (vanus.ID, uint64) {
	return r.leaderID, r.log.HardState().Term
}

// CloseWrite implements SegmentBlockWriter.
func (r *replica) CloseWrite(ctx context.Context) error {
	// return r.appender.CloseWrite(ctx)
	return nil
}

func (r *replica) isLeader() bool {
	return r.leaderID == r.blockID
}
