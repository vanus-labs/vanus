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

package raft

import (
	// standard libraries.
	"context"

	// this project.
	pb "github.com/linkall-labs/vanus/raft/raftpb"
)

type ProposeCallback = func(error)

type ProposeData struct {
	Type         pb.EntryType
	Data         []byte
	Callback     ProposeCallback
	NoWaitCommit bool
}

type ProposeDataOption func(cfg *ProposeData)

func Data(data []byte) ProposeDataOption {
	return func(pd *ProposeData) {
		pd.Data = data
	}
}

func Callback(cb ProposeCallback) ProposeDataOption {
	return func(pd *ProposeData) {
		pd.Callback = cb
	}
}

func NoWaitCommit() ProposeDataOption {
	return func(pd *ProposeData) {
		pd.NoWaitCommit = true
	}
}

type ProposeOption func(cfg *ProposeData)

func WithData(opts ...ProposeDataOption) ProposeOption {
	return func(cfg *ProposeData) {
		for _, opt := range opts {
			opt(cfg)
		}
	}
}

func Propose(ctx context.Context, n Node, opts ...ProposeOption) {
	pds := make([]ProposeData, len(opts))
	for i, opt := range opts {
		opt(&pds[i])
	}
	n.Propose(ctx, pds...)
}

type proposeFuture chan error

func newProposeFuture() proposeFuture {
	return make(proposeFuture, 1)
}

func (pf proposeFuture) onProposed(err error) {
	if err != nil {
		pf <- err
	}
	close(pf)
}

func (pf proposeFuture) wait() error {
	return <-pf
}

func Propose0(ctx context.Context, n Node, data []byte) error {
	future := newProposeFuture()
	n.Propose(ctx, ProposeData{Data: data, Callback: future.onProposed})
	return future.wait()
}

func Propose1(ctx context.Context, n Node, data []byte) error {
	future := newProposeFuture()
	n.Propose(ctx, ProposeData{Data: data, Callback: future.onProposed, NoWaitCommit: true})
	return future.wait()
}

func Propose2(ctx context.Context, n Node, data []byte) {
	n.Propose(ctx, ProposeData{Data: data})
}
