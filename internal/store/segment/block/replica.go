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

package block

import (
	// standard libraries.
	"context"
	"sort"
	"sync"
	"time"

	// first-party libraries.
	"github.com/linkall-labs/raft"
	"github.com/linkall-labs/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
)

type IDAndEndpoint struct {
	ID       vanus.ID
	Endpoint string
}

type idAndEndpoint struct {
	id       uint64
	endpoint string
}

type Replica struct {
	num  int
	wo   int
	full bool
	mu   sync.RWMutex

	block *fileBlock

	isLeader bool
	leaderID vanus.ID

	node   raft.Node
	log    *raftlog.Log
	sender transport.Sender

	endpoints []idAndEndpoint
	epMu      sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	donec  chan struct{}
}

// Make sure replica implements SegmentBlockWriter and transport.Receiver.
var _ SegmentBlockWriter = (*Replica)(nil)
var _ transport.Receiver = (*Replica)(nil)

func NewReplica(ctx context.Context, block SegmentBlock, raftLog *raftlog.Log, sender transport.Sender) *Replica {
	blockID := block.SegmentBlockID()

	// TODO(james.yin): Recover the in-memory storage from persistent snapshot, state and entries.
	// log.ApplySnapshot(snapshot)
	// log.SetHardState(state)

	ctx, cancel := context.WithCancel(ctx)

	r := &Replica{
		block:     block.(*fileBlock),
		log:       raftLog,
		sender:    sender,
		endpoints: make([]idAndEndpoint, 0, 2),
		ctx:       ctx,
		cancel:    cancel,
		donec:     make(chan struct{}),
	}
	r.resetByBlock()

	c := &raft.Config{
		ID:                        blockID.Uint64(),
		ElectionTick:              10,
		HeartbeatTick:             3,
		Storage:                   raftLog,
		MaxSizePerMsg:             4096,
		MaxInflightMsgs:           256,
		DisableProposalForwarding: true,
	}
	r.node = raft.RestartNode(c)
	go r.run()

	return r
}

func (r *Replica) Stop() {
	r.cancel()
	// Block until the stop has been acknowledged.
	<-r.donec
}

func (r *Replica) Bootstrap(blocks []IDAndEndpoint) error {
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

func (r *Replica) run() {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	var hardState raftpb.HardState
	for {
		select {
		case <-t.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			// TODO(james.yin): hard state
			if !raft.IsEmptyHardState(rd.HardState) {
				hardState = rd.HardState
			}

			if err := r.log.Append(rd.Entries); err != nil {
				panic(err)
			}

			if rd.SoftState != nil {
				r.leaderID = vanus.NewIDFromUint64(rd.SoftState.Lead)
				isLeader := rd.SoftState.RaftState == raft.StateLeader
				// reset when become leader
				if isLeader {
					r.reset(hardState)
				}
				r.isLeader = isLeader
			}

			// NOTE: Messages to be sent AFTER HardState and Entries
			// are committed to stable storage.
			r.send(rd.Messages)

			// TODO(james.yin): snapshot
			if !raft.IsEmptySnap(rd.Snapshot) {
				// processSnapshot(rd.Snapshot)
			}

			for _, entrypb := range rd.CommittedEntries {
				if entrypb.Type == raftpb.EntryNormal {
					// Skip empty entry(raft heartbeat).
					if len(entrypb.Data) > 0 {
						r.doAppend(entrypb)
					}
					continue
				}

				// change membership
				var cci raftpb.ConfChangeI
				if entrypb.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entrypb.Data); err != nil {
						panic(err)
					}
					// TODO(james.yin): non-add
					r.hintEndpoint(cc.NodeID, string(cc.Context))
					cci = cc
				} else {
					var cc raftpb.ConfChangeV2
					if err := cc.Unmarshal(entrypb.Data); err != nil {
						panic(nil)
					}
					// TODO(james.yin): non-add
					for _, ccs := range cc.Changes {
						r.hintEndpoint(ccs.NodeID, string(cc.Context))
					}
					cci = cc
				}
				r.node.ApplyConfChange(cci)
			}

			r.node.Advance()
		case <-r.ctx.Done():
			r.node.Stop()
			close(r.donec)
			return
		}
	}
}

func (r *Replica) reset(hardState raftpb.HardState) {
	off, err := r.log.LastIndex()
	if err != nil {
		off = hardState.Commit
	}

	for off > 0 {
		entrypbs, err := r.log.Entries(off, off+1, 0)

		// compacted
		if err != nil {
			r.resetByBlock()
			break
		}

		entrypb := entrypbs[0]
		if entrypb.Type == raftpb.EntryNormal && len(entrypb.Data) > 0 {
			entry := UnmarshalWithOffsetAndIndex(entrypb.Data)
			r.resetByEntry(entry)
			break
		}

		off -= 1
	}

	// no normal entry
	if off == 0 {
		r.resetByBlock()
	}
}

func (r *Replica) resetByEntry(entry Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.num = int(entry.Index + 1)
	r.wo = int(entry.Offset) + len(entry.Payload)
	r.full = len(entry.Payload) == 0
}

func (r *Replica) resetByBlock() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.num = int(r.block.num.Load())
	r.wo = int(r.block.wo.Load())
	r.full = r.block.full.Load()
}

// Append implements SegmentBlockWriter.
func (r *Replica) Append(ctx context.Context, entries ...Entry) error {
	if !r.isLeader {
		return ErrNotLeader
	}

	// TODO(james.yin): support batch
	if len(entries) != 1 {
		return errors.ErrInvalidRequest
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.preAppend(ctx, entries); err != nil {
		return err
	}

	// FIXME(james.yin): batch propose
	data := entries[0].MarshalWithOffsetAndIndex()
	if err := r.node.Propose(ctx, data); err != nil {
		return err
	}

	// TODO(james.yin): wait committed

	return nil
}

func (r *Replica) preAppend(ctx context.Context, entries []Entry) error {
	if r.full {
		return ErrFull
	}

	var size int
	for i := range entries {
		entry := &entries[i]
		entry.Offset = uint32(r.wo + size)
		entry.Index = uint32(r.num + i)
		size += entry.Size()
	}

	// TODO(james.yin): full
	if int64(r.wo+size+v1IndexLength*(r.num+len(entries))) > r.block.cap {
		fullEntry := Entry{
			Offset: uint32(r.wo),
			Index:  uint32(r.num),
		}
		data := fullEntry.MarshalWithOffsetAndIndex()
		if err := r.node.Propose(ctx, data); err != nil {
			return err
		}
		r.full = true
		return ErrNoEnoughCapacity
	}

	r.wo += size
	r.num += len(entries)

	return nil
}

func (r *Replica) doAppend(entrypb raftpb.Entry) {
	entry := UnmarshalWithOffsetAndIndex(entrypb.Data)

	// TODO(james.yin): full
	if len(entry.Payload) <= 0 {
		r.full = true
		r.block.full.Store(true)
		return
	}

	// FIXME(james.yin): context
	r.block.Append(context.Background(), entry)
}

func (r *Replica) send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if len(msgs) == 1 {
		msg := &msgs[0]
		endpoint := r.endpointHint(msg.To)
		r.sender.Send(r.ctx, msg, msg.To, endpoint)
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
		ma := make([]*raftpb.Message, len(msgs))
		for i := 0; i < len(msgs); i++ {
			ma[i] = &msgs[i]
		}
		endpoint := r.endpointHint(to)
		r.sender.Sendv(r.ctx, ma, to, endpoint)
		return
	}

	mm := make(map[uint64][]*raftpb.Message)
	for i := 0; i < len(msgs); i++ {
		msg := &msgs[i]
		mm[msg.To] = append(mm[msg.To], msg)
	}
	for to, msgs := range mm {
		endpoint := r.endpointHint(to)
		r.sender.Sendv(r.ctx, msgs, to, endpoint)
	}
}

func (r *Replica) endpointHint(to uint64) string {
	r.epMu.RLock()
	defer r.epMu.RUnlock()
	for i := range r.endpoints {
		ep := &r.endpoints[i]
		if ep.id == to {
			return ep.endpoint
		}
	}
	return ""
}

// Receive implements transport.Receiver.
func (r *Replica) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	if endpoint != "" {
		r.hintEndpoint(from, endpoint)
	}

	// TODO(james.yin): check ctx.Done().
	r.node.Step(r.ctx, *msg)
}

func (r *Replica) hintEndpoint(from uint64, endpoint string) {
	if endpoint == "" {
		return
	}

	// TODO(james.yin): optimize lock
	r.epMu.Lock()
	defer r.epMu.Unlock()
	ep := func() *idAndEndpoint {
		for i := range r.endpoints {
			ep := &r.endpoints[i]
			if ep.id == from {
				return ep
			}
		}
		return nil
	}()
	if ep == nil {
		r.endpoints = append(r.endpoints, idAndEndpoint{
			id:       from,
			endpoint: endpoint,
		})
	} else if ep.endpoint != endpoint {
		ep.endpoint = endpoint
	}
}

// CloseWrite implements SegmentBlockWriter.
func (r *Replica) CloseWrite(ctx context.Context) error {
	return r.block.CloseWrite(ctx)
}

// IsAppendable implements SegmentBlockWriter.
func (r *Replica) IsAppendable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !r.full
}
