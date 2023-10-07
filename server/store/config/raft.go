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

package config

import (
	// this project.
	"github.com/vanus-labs/vanus/server/store/raft/block"
)

const minRaftLogWALFileSize uint64 = 32 * baseMB

type RaftExecutorParallel struct {
	Raft      int `yaml:"raft"`
	Append    int `yaml:"append"`
	Commit    int `yaml:"commit"`
	Persist   int `yaml:"persist"`
	Apply     int `yaml:"apply"`
	Transport int `yaml:"transport"`
}

type Raft struct {
	WAL      WAL                  `yaml:"wal"`
	Parallel RaftExecutorParallel `yaml:"parallel"`
}

func (c *Raft) Validate() error {
	return c.WAL.Validate(minRaftLogWALFileSize)
}

func (c *Raft) Options() (opts []block.Option) {
	opts = append(opts, block.WithWALOptions(c.WAL.Options()...))
	if c.Parallel.Raft != 0 {
		opts = append(opts, block.WithRaftExecutorParallel(c.Parallel.Raft))
	}
	if c.Parallel.Append != 0 {
		opts = append(opts, block.WithAppendExecutorParallel(c.Parallel.Append))
	}
	if c.Parallel.Commit != 0 {
		opts = append(opts, block.WithCommitExecutorParallel(c.Parallel.Commit))
	}
	if c.Parallel.Persist != 0 {
		opts = append(opts, block.WithPersistExecutorParallel(c.Parallel.Persist))
	}
	if c.Parallel.Apply != 0 {
		opts = append(opts, block.WithApplyExecutorParallel(c.Parallel.Append))
	}
	if c.Parallel.Transport != 0 {
		opts = append(opts, block.WithTransportExecutorParallel(c.Parallel.Transport))
	}
	return opts
}
