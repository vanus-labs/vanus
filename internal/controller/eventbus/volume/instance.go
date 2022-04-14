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

package volume

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/block"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"time"
)

type Instance interface {
	GetMeta() *Metadata
	ID() uint64
	Address() string
	CreateBlock(context.Context, int64) (*block.Block, error)
	DeleteBlock(context.Context, uint64) error
	ActivateSegment(context.Context, *Segment) error
	Close() error
}

func newInstance(md *Metadata) Instance {
	return nil
}

type instance struct {
	md       *Metadata
	srv      Server
	grpcConn *grpc.ClientConn
	client   segment.SegmentServerClient
}

func (ins *instance) GetMeta() *Metadata {
	return ins.md
}

func (ins *instance) CreateBlock(ctx context.Context, cap int64) (*block.Block, error) {
	blk := &block.Block{
		ID:       ins.generateBlockID(),
		Capacity: cap,
	}
	_, err := ins.client.CreateBlock(ctx, &segment.CreateBlockRequest{
		Size: blk.Capacity,
		Id:   blk.ID,
	})
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (ins *instance) DeleteBlock(context.Context, uint64) error {
	return nil
}

func (ins *instance) UpdateSegmentServer(srv Server) {
	ins.srv = srv
}

func (ins *instance) ActivateSegment(ctx context.Context, seg *Segment) error {
	_, err := ins.client.ActivateSegment(ctx, &segment.ActivateSegmentRequest{
		EventLogId:     seg.EventLogID,
		ReplicaGroupId: seg.Replicas.ID,
		PeersAddress:   seg.Replicas.Peers(),
	})
	return err
}

func (ins *instance) Address() string {
	if ins.srv == nil {
		return ""
	}
	return ins.srv.Address()
}

func (ins *instance) Close() error {
	return ins.grpcConn.Close()
}

func (ins *instance) generateBlockID() uint64 {
	//TODO optimize
	return uint64(time.Now().UnixNano())
}
