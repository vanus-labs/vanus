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

	// this project.
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultDirPerm = 0755
)

func RecoverWithVisitor(walDir string, compacted int64, visitor WalkFunc) (*WAL, error) {
	// Make sure the WAL directory exists.
	if err := os.MkdirAll(walDir, defaultDirPerm); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	files = filterRegularLog(files)

	s := &logStream{
		dir: walDir,
	}
	for _, file := range files {
		filename := file.Name()
		so, err2 := strconv.ParseInt(filename[:len(filename)-len(logFileExt)-1], 10, 64)
		if err2 != nil {
			return nil, err2
		}

		if f := s.lastFile(); f != nil {
			eo := f.so + f.size
			// discontinuous log file
			if so != eo {
				log.Warning(context.TODO(), "Discontinuous log file, discard before.",
					map[string]interface{}{
						"lastEnd":   eo,
						"nextStart": so,
					})
				s.stream = nil
			}
		}

		info, err2 := file.Info()
		if err2 != nil {
			return nil, err2
		}

		path := filepath.Join(walDir, filename)
		size := info.Size()
		if size%blockSize != 0 {
			truncated := size - size%blockSize
			log.Warning(context.TODO(), "The size of log file is not a multiple of blockSize, truncate it.",
				map[string]interface{}{
					"file":       path,
					"originSize": size,
					"newSize":    truncated,
				})
			size = truncated
		}

		s.stream = append(s.stream, &logFile{
			so:   so,
			size: size,
			path: path,
		})
	}

	eo, err := s.Visit(visitor, compacted)
	if err != nil {
		return nil, err
	}

	// Make WAL.
	return newWAL(context.TODO(), s, eo)
}

func filterRegularLog(entries []os.DirEntry) []os.DirEntry {
	if len(entries) == 0 {
		return entries
	}

	n := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if filepath.Ext(entry.Name()) != logFileExt {
			continue
		}
		entries[n] = entry
		n++
	}
	entries = entries[:n]
	return entries
}
