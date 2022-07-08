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
	defaultDirPerm = 0o755
)

func RecoverWithVisitor(walDir string, compacted int64, visitor OnEntryCallback, opts ...Option) (*WAL, error) {
	// Make sure the WAL directory exists.
	if err := os.MkdirAll(walDir, defaultDirPerm); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	files = filterRegularLog(files)

	cfg := makeConfig(walDir, opts...)

	// Rebuild log stream.
	s := cfg.stream
	var last *logFile
	for _, file := range files {
		filename := file.Name()
		so, err2 := strconv.ParseInt(filename[:len(filename)-len(logFileExt)], 10, 64)
		if err2 != nil {
			return nil, err2
		}

		if last != nil {
			// discontinuous log file
			if so != last.eo() {
				log.Warning(context.Background(), "Discontinuous log file, discard before.",
					map[string]interface{}{
						"last_end":   last.eo,
						"next_start": so,
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
		if size%s.blockSize != 0 {
			truncated := size - size%s.blockSize
			log.Warning(context.Background(), "The size of log file is not a multiple of blockSize, truncate it.",
				map[string]interface{}{
					"file":       path,
					"originSize": size,
					"newSize":    truncated,
				})
			size = truncated
		}

		last = &logFile{
			so:   so,
			size: size,
			path: path,
		}
		s.stream = append(s.stream, last)
	}

	pos, err := s.Range(compacted, visitor)
	if err != nil {
		return nil, err
	}
	WithPosition(pos)(&cfg)

	// Make WAL.
	return newWAL(cfg)
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
