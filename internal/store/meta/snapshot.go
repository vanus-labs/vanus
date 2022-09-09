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
	"os"
	"path/filepath"
	"strconv"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
)

const (
	snapshotExt         = ".snapshot"
	defaultSnapshotPrem = 0o644
	defaultDirPerm      = 0o755
)

func (s *store) tryCreateSnapshot(ctx context.Context) {
	ctx, span := s.tracer.Start(ctx, "tryCreateSnapshot")
	defer span.End()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.needCreateSnapshot() {
		s.createSnapshot(ctx)
	}
}

func (s *store) needCreateSnapshot() bool {
	// TODO(james.yin): create snapshot condition
	return s.snapshot-s.version > 4*1024*1024
}

func (s *store) createSnapshot(_ context.Context) {
	data, err := s.marshaler.Marshal(SkiplistRange(s.committed))
	if err != nil {
		return
	}

	// Write data to file.
	path := s.resolveSnapshotPath(s.version)
	if err = os.WriteFile(path, data, defaultSnapshotPrem); err != nil {
		log.Warning(context.TODO(), "Write snapshot failed.", map[string]interface{}{
			"path":  path,
			"error": err,
		})
		return
	}
	lastSnapshot := s.snapshot
	s.snapshot = s.version

	// Compact expired wal.
	_ = s.wal.Compact(s.snapshot)
	_ = os.Remove(s.resolveSnapshotPath(lastSnapshot))
}

func (s *store) resolveSnapshotPath(version int64) string {
	return filepath.Join(s.wal.Dir(), fmt.Sprintf("%020d%s", version, snapshotExt))
}

func recoverLatestSnapshot(ctx context.Context, dir string,
	unmarshaler Unmarshaler) (*skiplist.SkipList, int64, error) {
	// Make sure the snapshot directory exists.
	_, span := tracing.Start(ctx, "store.meta.snapshot", "recoverLatestSnapshot")
	defer span.End()

	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return nil, 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	latest, expired := filterLatestSnapshot(files)

	if latest == nil {
		return skiplist.New(skiplist.Bytes), 0, nil
	}

	filename := latest.Name()
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
	err = unmarshaler.Unmarshal(data, func(key []byte, value interface{}) error {
		m.Set(key, value)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	// Delete expired snapshots.
	for _, entry := range expired {
		_ = os.Remove(filepath.Join(dir, entry.Name()))
	}

	return m, snapshot, nil
}

func filterLatestSnapshot(entries []os.DirEntry) (os.DirEntry, []os.DirEntry) {
	if len(entries) == 0 {
		return nil, nil
	}

	var snapshots []os.DirEntry
	for i := 1; i <= len(entries); i++ {
		entry := entries[len(entries)-i]
		if !entry.Type().IsRegular() {
			continue
		}
		if filepath.Ext(entry.Name()) != snapshotExt {
			continue
		}
		snapshots = append(snapshots, entry)
	}

	if len(snapshots) == 0 {
		return nil, nil
	}
	return snapshots[0], snapshots[1:]
}
