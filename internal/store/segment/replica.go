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

//go:generate mockgen -source=replica.go  -destination=mock_replica.go -package=segment
package segment

import (
	// standard libraries.
	"context"

	// first-party libraries.
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/block/raft"
	"github.com/linkall-labs/vanus/internal/store/block/raw"
)

type Replica interface {
	block.Block

	IDStr() string
	Bootstrap(ctx context.Context, blocks []raft.Peer) error
	Close(ctx context.Context) error
	Delete(ctx context.Context) error
	Status() *metapb.SegmentHealthInfo
}

type replica struct {
	id       vanus.ID
	idStr    string
	engine   raw.Engine
	raw      block.Raw
	appender raft.Appender
}

var _ Replica = (*replica)(nil)

func (r *replica) ID() vanus.ID {
	return r.id
}

func (r *replica) IDStr() string {
	return r.idStr
}

func (r *replica) Bootstrap(ctx context.Context, blocks []raft.Peer) error {
	return r.appender.Bootstrap(ctx, blocks)
}

func (r *replica) Close(ctx context.Context) error {
	r.appender.Stop(ctx)
	return r.raw.Close(ctx)
}

func (r *replica) Delete(ctx context.Context) error {
	r.appender.Delete(ctx)
	return r.raw.Delete(ctx)
}

func (r *replica) Read(ctx context.Context, seq int64, num int) ([]block.Entry, error) {
	return r.raw.Read(ctx, seq, num)
}

func (r *replica) Append(ctx context.Context, entries ...block.Entry) ([]int64, error) {
	return r.appender.Append(ctx, entries...)
}

func (r *replica) Status() *metapb.SegmentHealthInfo {
	stat, _ := r.engine.GetBlockStatistics(r.id, r.raw)
	cs := r.appender.Status()

	// TODO(james.yin): fill EntLogId and SerializationVersion.
	info := &metapb.SegmentHealthInfo{
		Id:                 r.id.Uint64(),
		Capacity:           int64(stat.Capacity),
		Size:               int64(stat.EntrySize),
		EventNumber:        int32(stat.EntryNum),
		IsFull:             stat.Archived,
		Leader:             cs.Leader.Uint64(),
		Term:               cs.Term,
		FirstEventBornTime: stat.FirstEntryStime,
	}
	if stat.Archived {
		info.LastEventBornTime = stat.LastEntryStime
	}
	return info
}

func (s *server) createBlock(ctx context.Context, id vanus.ID, size int64) (Replica, error) {
	e, _ := raw.ResolveEngine(raw.VSB)

	// Create block.
	r, err := e.Create(ctx, id, size)
	if err != nil {
		return nil, err
	}

	// Create replica.
	l := raftlog.NewLog(id, s.wal, s.metaStore, s.offsetStore, nil)
	a := raft.NewAppender(context.TODO(), r, l, s.host, s.leaderChanged)

	return &replica{
		id:       id,
		idStr:    id.String(),
		engine:   e,
		raw:      r,
		appender: a,
	}, nil
}
