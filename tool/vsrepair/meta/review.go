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

package meta

import (
	// standard libraries.
	"context"
	"encoding/binary"
	"path/filepath"

	// this project.
	"github.com/vanus-labs/vanus/server/store/meta"
	"github.com/vanus-labs/vanus/server/store/raft/storage"
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

func ReviewCompact(volumeDir string, node uint64, watcher func(*CompactInfo, int64)) error {
	ctx := context.Background()
	dir := filepath.Join(volumeDir, "meta")
	comKey := []byte(storage.CompactKey(node))

	return meta.ReviewSyncStore(ctx, dir, comKey, func(v interface{}, ver int64) {
		if v == meta.DeletedMark {
			watcher(nil, ver)
		} else {
			com, ok := v.([]byte)
			if !ok {
				panic("compacted is not []byte")
			}
			info := CompactInfo{
				Index: binary.BigEndian.Uint64(com[0:8]),
				Term:  binary.BigEndian.Uint64(com[8:16]),
			}
			watcher(&info, ver)
		}
	}, walog.WithReadOnly())
}
