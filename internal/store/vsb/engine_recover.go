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
	"context"
	"os"
	"path/filepath"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
)

func (e *engine) Recover(ctx context.Context) (map[vanus.ID]block.Raw, error) {
	files, err := os.ReadDir(e.dir)
	if err != nil {
		return nil, err
	}
	files = filterRegularBlock(files)

	blocks := make(map[vanus.ID]block.Raw, len(files))
	for _, file := range files {
		filename := file.Name()
		blockID, err2 := vanus.NewIDFromString(filename[:len(filename)-len(vsbExt)])
		if err2 != nil {
			// TODO(james.yin): skip this file?
			err = err2
			break
		}

		block, err2 := e.Open(ctx, blockID)
		if err2 != nil {
			err = err2
			break
		}
		blocks[blockID] = block
	}

	if err != nil {
		for _, block := range blocks {
			_ = block.Close(ctx)
		}
		return nil, err
	}

	return blocks, nil
}

func filterRegularBlock(entries []os.DirEntry) []os.DirEntry {
	if len(entries) == 0 {
		return entries
	}

	n := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if filepath.Ext(entry.Name()) != vsbExt {
			continue
		}
		entries[n] = entry
		n++
	}
	entries = entries[:n]
	return entries
}
