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

package segmentedfile

import (
	// standard libraries.
	"context"
	"os"
	"path/filepath"
	"strconv"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
)

const (
	defaultDirPerm = 0o755
)

// recoverSegments rebuilds segments from specified directory.
func recoverSegments(dir string, cfg config) ([]*Segment, error) {
	// Make sure the directory exists.
	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return nil, err
	}

	segments, discards, err := scanSegmentFiles(dir, cfg.ext, cfg.segmentSize)
	if err != nil {
		return nil, err
	}

	// Delete discard files.
	for _, s := range discards {
		s.Close()
		_ = os.Remove(s.path)
	}

	return segments, nil
}

func scanSegmentFiles(dir, ext string, segmentSize int64) (segments []*Segment, discards []*Segment, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	files = filterRegularFiles(files, ext)

	// Rebuild log stream.
	var last *Segment
	for _, file := range files {
		filename := file.Name()
		so, err2 := strconv.ParseInt(filename[:len(filename)-len(ext)], 10, 64)
		if err2 != nil {
			return nil, nil, err2
		}

		if last != nil {
			// discontinuous log file
			if so != last.eo {
				log.Warning(context.Background(), "Discontinuous segment, discard before.",
					map[string]interface{}{
						"last_end":   last.eo,
						"next_start": so,
					})
				discards = append(discards, segments...)
				segments = nil
			}
		}

		info, err2 := file.Info()
		if err2 != nil {
			return nil, nil, err2
		}

		path := filepath.Join(dir, filename)
		size := info.Size()

		if size%segmentSize != 0 {
			// TODO(james.yin): return error
			truncated := size - size%segmentSize
			log.Warning(context.Background(), "The size of log file is not a multiple of blockSize, truncate it.",
				map[string]interface{}{
					"file":        path,
					"origin_size": size,
					"new_size":    truncated,
				})
			size = truncated
		}

		f, err2 := io.OpenFile(path, os.O_RDWR, true, true)
		if err2 != nil {
			return nil, nil, err2
		}

		last = newSegment(path, so, size, f)
		segments = append(segments, last)
	}

	return segments, discards, nil
}

func filterRegularFiles(entries []os.DirEntry, ext string) []os.DirEntry {
	if len(entries) == 0 {
		return entries
	}

	n := 0
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if filepath.Ext(entry.Name()) != ext {
			continue
		}
		entries[n] = entry
		n++
	}
	entries = entries[:n]
	return entries
}
