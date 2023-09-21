// Copyright 2023 Linkall Inc.
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
	// this project.
	"github.com/vanus-labs/vanus/lib/executor"
)

type executorFactory struct {
	raftExecutor      executor.MultiFlow
	appendExecutor    executor.MultiFlow
	commitExecutor    executor.MultiFlow
	persistExecutor   executor.MultiFlow
	applyExecutor     executor.MultiFlow
	transportExecutor executor.MultiFlow
}

func newExecutorFactory(cfg executorConfig, startImmediately bool) *executorFactory {
	return new(executorFactory).init(cfg, startImmediately)
}

func (k *executorFactory) init(cfg executorConfig, startImmediately bool) *executorFactory {
	k.raftExecutor.Init(cfg.raftExecutorParallel, false, startImmediately)
	k.appendExecutor.Init(cfg.appendExecutorParallel, true, startImmediately)
	k.commitExecutor.Init(cfg.commitExecutorParallel, false, startImmediately)
	k.persistExecutor.Init(cfg.persistExecutorParallel, false, startImmediately)
	k.applyExecutor.Init(cfg.applyExecutorParallel, false, startImmediately)
	k.transportExecutor.Init(cfg.transportExecutorParallel, false, startImmediately)
	return k
}

func (k *executorFactory) start() {
	k.raftExecutor.Start()
	k.appendExecutor.Start()
	k.commitExecutor.Start()
	k.persistExecutor.Start()
	k.applyExecutor.Start()
	k.transportExecutor.Start()
}

func (k *executorFactory) newRaftFlow() executor.ExecuteCloser {
	return k.raftExecutor.NewFlow()
}

func (k *executorFactory) newAppendFlow() executor.ExecuteCloser {
	return k.appendExecutor.NewFlow()
}

func (k *executorFactory) newCommitFlow() executor.ExecuteCloser {
	return k.commitExecutor.NewFlow()
}

func (k *executorFactory) newPersistFlow() executor.ExecuteCloser {
	return k.persistExecutor.NewFlow()
}

func (k *executorFactory) newApplyFlow() executor.ExecuteCloser {
	return k.applyExecutor.NewFlow()
}

func (k *executorFactory) newTransportFlow() executor.ExecuteCloser {
	return k.transportExecutor.NewFlow()
}
