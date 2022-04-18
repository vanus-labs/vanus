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
	"time"

	// third-party libraries.
	"github.com/linkall-labs/raft"
	"github.com/linkall-labs/raft/raftpb"

	// this project.
	vsraft "github.com/linkall-labs/vanus/internal/raft"
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/internal/store/segment/codec"
	"github.com/linkall-labs/vanus/internal/store/wal"
)

type replica struct {
	block  SegmentBlock
	node   raft.Node
	log    *vsraft.Log
	sender transport.Sender

	ctx    context.Context
	cancel context.CancelFunc
	donec  chan struct{}
}

// Make sure replica implements SegmentBlock, raft.Receiver.
// var _ *SegmentBlock = (*replica)(nil)
var _ transport.Receiver = (*replica)(nil)

func newReplica(ctx context.Context, b SegmentBlock, w *wal.WAL, s transport.Sender) *replica {
	var blockID uint64 = 1

	// TODO: set peers
	log := vsraft.NewLog(blockID, w, []uint64{})

	// TODO(james.yin): Recover the in-memory storage from persistent snapshot, state and entries.
	// log.ApplySnapshot(snapshot)
	// log.SetHardState(state)
	// log.Append(entries)

	ctx, cancel := context.WithCancel(ctx)

	r := &replica{
		block:  b,
		log:    log,
		sender: s,
		ctx:    ctx,
		cancel: cancel,
		donec:  make(chan struct{}),
	}

	// FIXME(james.yin): ID
	c := &raft.Config{
		ID:              blockID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         log,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	r.node = raft.RestartNode(c)

	return r
}

func (r *replica) Stop() {
	r.cancel()
	// Block until the stop has been acknowledged.
	<-r.donec
}

func (r *replica) Bootstrap() {
	// TODO
	peers := []raft.Peer{}
	err := r.node.Bootstrap(peers)
	if err != nil {
		// TODO
	}
}

func (r *replica) run() {
	// TODO(james.yin): reduce Ticker
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			r.node.Tick()
		case rd := <-r.node.Ready():
			// TODO(james.yin): hard state, snapshot
			r.log.Append(rd.Entries)

			// NOTE: Messages to be sent AFTER HardState and Entries
			// are committed to stable storage.
			r.send(rd.Messages)

			// TODO(james.yin): snapshot
			// if !raft.IsEmptySnap(rd.Snapshot) {
			// 	processSnapshot(rd.Snapshot)
			// }

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryNormal {
					r.doAppend(entry)
					continue
				}

				// change membership
				var cci raftpb.ConfChangeI
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						panic(err)
					}
					cci = cc
				} else {
					var cc raftpb.ConfChangeV2
					if err := cc.Unmarshal(entry.Data); err != nil {
						panic(nil)
					}
					cci = cc
				}
				r.node.ApplyConfChange(cci)
			}

			r.node.Advance()
		case <-r.ctx.Done():
			r.node.Stop()
			return
		}
	}
}

func (r *replica) Append(ctx context.Context, entries ...*codec.StoredEntry) error {
	if len(entries) != 1 {
		// TODO
		return nil
	}

	// FIXME
	err := r.node.Propose(ctx, entries[0].Payload)
	if err != nil {
		return err
	}

	// TODO: wait committed

	return nil
}

func (r *replica) doAppend(entry raftpb.Entry) {
	// FIXME
	r.block.Append(context.Background(), &codec.StoredEntry{
		Payload: entry.Data,
	})
}

func (r *replica) send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if len(msgs) == 1 {
		msg := &msgs[0]
		r.sender.Send(r.ctx, msg, msg.To)
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
		r.sender.Sendv(r.ctx, ma, to)
		return
	}

	mm := make(map[uint64][]*raftpb.Message)
	for i := 0; i < len(msgs); i++ {
		msg := &msgs[i]
		mm[msg.To] = append(mm[msg.To], msg)
	}
	for to := range mm {
		r.sender.Sendv(r.ctx, mm[to], to)
	}
}

// Receive implements transport.Receiver.
func (r *replica) Receive(ctx context.Context, msg *raftpb.Message, from uint64) {
	// TODO: check ctx.Done().
	r.node.Step(r.ctx, *msg)
}
