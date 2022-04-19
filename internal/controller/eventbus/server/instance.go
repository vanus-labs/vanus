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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"sync"
)

type Instance interface {
	ID() vanus.ID
	Address() string
	Close() error
	GetMeta() *metadata.VolumeMetadata
	CreateBlock(context.Context, int64) (*metadata.Block, error)
	DeleteBlock(context.Context, vanus.ID) error
	GetServer() Server
	SetServer(Server)
}

func NewInstance(md *metadata.VolumeMetadata) Instance {
	return &volumeInstance{
		md: md,
	}
}

type volumeInstance struct {
	md      *metadata.VolumeMetadata
	srv     Server
	rwMutex sync.RWMutex
}

func (ins *volumeInstance) GetMeta() *metadata.VolumeMetadata {
	return ins.md
}

func (ins *volumeInstance) CreateBlock(ctx context.Context, cap int64) (*metadata.Block, error) {
	blk := &metadata.Block{
		ID:       vanus.NewID(),
		Capacity: cap,
		VolumeID: ins.md.ID,
	}
	if ins.srv == nil {
		return nil, errors.ErrVolumeInstanceNoServer
	}
	_, err := ins.srv.GetClient().CreateBlock(ctx, &segpb.CreateBlockRequest{
		Size: blk.Capacity,
		Id:   blk.ID.Uint64(),
	})
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (ins *volumeInstance) DeleteBlock(ctx context.Context, id vanus.ID) error {
	if ins.srv == nil {
		return errors.ErrVolumeInstanceNoServer
	}
	_, err := ins.srv.GetClient().RemoveBlock(ctx, &segpb.RemoveBlockRequest{Id: id.Uint64()})
	return err
}

func (ins *volumeInstance) ID() vanus.ID {
	return ins.md.ID
}

func (ins *volumeInstance) Address() string {
	ins.rwMutex.RLock()
	defer ins.rwMutex.RUnlock()

	if ins.srv != nil {
		return ins.srv.Address()
	}
	return ""
}

func (ins *volumeInstance) Close() error {
	ins.rwMutex.RLock()
	defer ins.rwMutex.RUnlock()

	if ins.srv != nil {
		return ins.srv.Close()
	}
	return nil
}

func (ins *volumeInstance) SetServer(srv Server) {
	ins.rwMutex.Lock()
	defer ins.rwMutex.Unlock()
	ins.srv = srv
}

func (ins *volumeInstance) GetServer() Server {
	ins.rwMutex.RLock()
	defer ins.rwMutex.RUnlock()

	return ins.srv
}
