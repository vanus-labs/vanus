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
	// standard libraries.
	"os"
	"path/filepath"

	// first-party libraries.
	vanus "github.com/vanus-labs/vanus/api/vsr"

	// this project.
	"github.com/vanus-labs/vanus/server/store/vsb"
)

func VSBDetail(volumeDir string, id uint64) (vsb.Header, error) {
	blockID := vanus.NewIDFromUint64(id)
	blockPath := vsb.BlockPath(filepath.Join(volumeDir, "block"), blockID)

	f, err := os.OpenFile(blockPath, os.O_RDONLY, 0)
	if err != nil {
		return vsb.Header{}, err
	}

	defer f.Close()

	return vsb.LoadHeader(f)
}
