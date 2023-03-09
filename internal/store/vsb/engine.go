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

package vsb

import (
	// standard libraries.
	"os"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/block"
	"github.com/vanus-labs/vanus/internal/store/block/raw"
	"github.com/vanus-labs/vanus/internal/store/io/stream"
)

const (
	defaultDirPerm = 0o755
)

type engine struct {
	dir string
	s   stream.Scheduler
	lis block.ArchivedListener
}

// Make sure engine implements raw.Engine.
var _ raw.Engine = (*engine)(nil)

func (e *engine) Close() {
	// TODO(james.yin): check me
	e.s.Close()
}

func Initialize(dir string, opts ...Option) error {
	cfg := makeConfig(opts...)
	return initialize(dir, cfg)
}

func initialize(dir string, cfg config) error {
	// Make sure the block directory exists.
	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return err
	}

	s := stream.NewScheduler(cfg.engine, cfg.streamSchedulerOptions()...)

	return raw.RegisterEngine(raw.VSB, &engine{
		dir: dir,
		s:   s,
		lis: cfg.lis,
	})
}
