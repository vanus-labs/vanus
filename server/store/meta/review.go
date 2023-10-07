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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	// this project.
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

var errFound = errors.New("found")

type ReviewWatcher = func(value interface{}, version int64)

func ReviewSyncStore(ctx context.Context, dir string, key []byte, watcher ReviewWatcher, opts ...walog.Option) error {
	snapshot, err := reviewLatestSnapshot(ctx, dir, defaultCodec, key, watcher)
	if err != nil {
		return err
	}

	opts = append([]walog.Option{
		walog.FromPosition(snapshot),
		walog.WithRecoveryCallback(func(data []byte, r walog.Range) error {
			err2 := defaultCodec.Unmarshal(data, func(k []byte, v interface{}) error {
				if bytes.Equal(k, key) {
					watcher(v, r.EO)
				}
				return nil
			})
			if err2 != nil {
				return err2
			}
			return nil
		}),
	}, opts...)
	wal, err := walog.Open(ctx, dir, opts...)
	if err != nil {
		return err
	}

	wal.Close()

	return nil
}

func reviewLatestSnapshot(
	_ context.Context, dir string, unmarshaler Unmarshaler, key []byte, watcher ReviewWatcher,
) (int64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	latest, _ := filterLatestSnapshot(files)

	if latest == nil {
		return 0, nil
	}

	filename := latest.Name()
	snapshot, err := strconv.ParseInt(filename[:len(filename)-len(snapshotExt)], 10, 64)
	if err != nil {
		return 0, err
	}

	path := filepath.Join(dir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	err = unmarshaler.Unmarshal(data, func(k []byte, v interface{}) error {
		if bytes.Equal(k, key) {
			watcher(v, -1)
			// TODO(james.yin): don't skip remaind data?
			return errFound
		}
		return nil
	})
	if err != nil && err != errFound { //nolint:errorlint // compare to errFound is ok.
		return 0, err
	}

	return snapshot, nil
}
