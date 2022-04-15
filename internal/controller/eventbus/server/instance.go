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

package server

import (
	"context"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"time"
)

type Instance interface {
	Server
	GetMeta() *metadata.VolumeMetadata
	CreateBlock(context.Context, int64) (*metadata.Block, error)
	DeleteBlock(context.Context, vanus.ID) error
}

func NewInstance(md *metadata.VolumeMetadata) Instance {
	return nil
}

type instance struct {
	md       *metadata.VolumeMetadata
	srv      Server
	grpcConn *grpc.ClientConn
	client   segment.SegmentServerClient
}

func (ins *instance) GetMeta() *metadata.VolumeMetadata {
	return ins.md
}

func (ins *instance) CreateBlock(ctx context.Context, cap int64) (*metadata.Block, error) {
	blk := &metadata.Block{
		ID:       ins.generateBlockID(),
		Capacity: cap,
	}
	_, err := ins.client.CreateBlock(ctx, &segment.CreateBlockRequest{
		Size: blk.Capacity,
		Id:   blk.ID.Uint64(),
	})
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (ins *instance) DeleteBlock(context.Context, vanus.ID) error {
	return nil
}

func (ins *instance) UpdateSegmentServer(srv Server) {
	ins.srv = srv
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

func (ins *instance) generateBlockID() vanus.ID {
	//TODO optimize
	return vanus.ID(time.Now().UnixNano())
}
