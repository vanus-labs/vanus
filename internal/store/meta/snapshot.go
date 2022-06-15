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

package meta

import (
	// standard libraries.
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	snapshotExt         = ".snapshot"
	defaultSnapshotPrem = 0644
	defaultDirPerm      = 0755
)

func (s *store) tryCreateSnapshot() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.needCreateSnapshot() {
		s.createSnapshot()
	}
}

func (s *store) needCreateSnapshot() bool {
	// TODO(james.yin): create snapshot condition
	return s.snapshot-s.version > 4*1024*1024
}

func (s *store) createSnapshot() {
	data, err := s.marshaler.Marshal(SkiplistRange(s.committed))
	if err != nil {
		return
	}

	// Write data to file.
	name := fmt.Sprintf("%020d%s", s.version, snapshotExt)
	path := path.Join(s.wal.Dir(), name)
	if err = ioutil.WriteFile(path, data, defaultSnapshotPrem); err != nil {
		log.Warning(context.TODO(), "Write snapshot failed.", map[string]interface{}{
			"path":  name,
			"error": err,
		})
	}
	s.snapshot = s.version
}

func recoverLatestSnopshot(dir string, unmarshaler Unmarshaler) (*skiplist.SkipList, int64, error) {
	// Make sure the snapshot directory exists.
	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return nil, 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	file := filterLatestSnapshot(files)

	if file == nil {
		return skiplist.New(skiplist.Bytes), 0, nil
	}

	filename := file.Name()
	snapshot, err := strconv.ParseInt(filename[:len(filename)-len(snapshotExt)], 10, 64)
	if err != nil {
		return nil, 0, err
	}

	path := filepath.Join(dir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	m := skiplist.New(skiplist.Bytes)
	err = unmarshaler.Unmarshl(data, func(key []byte, value interface{}) error {
		m.Set(key, value)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return m, snapshot, nil
}

func filterLatestSnapshot(entries []os.DirEntry) os.DirEntry {
	if len(entries) == 0 {
		return nil
	}

	for i := 1; i <= len(entries); i++ {
		entry := entries[len(entries)-i]
		if !entry.Type().IsRegular() {
			continue
		}
		if filepath.Ext(entry.Name()) == snapshotExt {
			return entry
		}
	}
	return nil
}
