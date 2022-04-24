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

package wal

import (
	// standard libraries.
	"context"
	"os"
	"path/filepath"
	"strconv"
)

func RecoverWithVisitor(walDir string, visitor WalkFunc) (*WAL, error) {
	// Make sure the WAL directory exists.
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	files = filterRegular(files)

	var stream logStream
	for i, file := range files {
		so, err := strconv.ParseInt(file.Name(), 10, 64)
		if err != nil {
			return nil, err
		}

		if i > 0 {
			f := stream.lastFile()
			if so != f.so+f.size {
				// TODO(james.yin): discontinuous log file
			}
		}

		info, err := file.Info()
		if err != nil {
			return nil, err
		}

		stream = append(stream, logFile{
			so:   so,
			size: info.Size(),
			path: filepath.Join(walDir, file.Name()),
		})
	}

	err = stream.Visit(visitor)
	if err != nil {
		return nil, err
	}

	// Make WAL.
	wal := NewWAL(context.TODO())
	// TODO(james.yin): recover write block

	return wal, nil
}

func filterRegular(entries []os.DirEntry) []os.DirEntry {
	if len(entries) <= 0 {
		return entries
	}

	n := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		entries[n] = entry
		n++
	}
	entries = entries[:n]
	return entries
}
