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
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/block/raw"
)

const (
	defaultDirPerm = 0o755
)

type engine struct {
	dir string
	lis block.ArchivedListener
}

// Make sure engine implements raw.Engine.
var _ raw.Engine = (*engine)(nil)

func (e *engine) GetBlockStatistics(id vanus.ID, r block.Raw) (block.Statistics, error) {
	if r != nil {
		b, _ := r.(*vsBlock)
		return b.status(), nil
	}
	// TODO(james.yin): get vsb by id.
	return block.Statistics{}, nil
}

func Initialize(dir string, lis block.ArchivedListener) error {
	// Make sure the block directory exists.
	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return err
	}

	return raw.RegisterEngine(raw.VSB, &engine{
		dir: dir,
		lis: lis,
	})
}
