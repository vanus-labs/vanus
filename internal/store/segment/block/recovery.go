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

package block

import (
	// standard libraries.
	"os"
	"path/filepath"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

const (
	defaultDirPerm = 0755
)

func RecoverBlocks(blockDir string) (map[vanus.ID]string, error) {
	// Make sure the block directory exists.
	if err := os.MkdirAll(blockDir, defaultDirPerm); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(blockDir)
	if err != nil {
		return nil, err
	}
	files = filterRegularBlock(files)

	blocks := make(map[vanus.ID]string, len(files))
	for _, file := range files {
		filename := file.Name()
		blockID, err2 := vanus.NewIDFromString(filename[:len(filename)-len(blockExt)])
		if err2 != nil {
			return nil, err2
		}

		path := filepath.Join(blockDir, filename)
		blocks[blockID] = path
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
		if filepath.Ext(entry.Name()) != blockExt {
			continue
		}
		entries[n] = entry
		n++
	}
	entries = entries[:n]
	return entries
}
