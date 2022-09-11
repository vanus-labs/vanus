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

	// third-party project.
	oteltracer "go.opentelemetry.io/otel/trace"

	// first-party project.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
)

const (
	defaultDirPerm = 0o755
)

// recoverLogStream rebuilds log stream from specified directory.
func recoverLogStream(ctx context.Context, dir string, cfg config) (*logStream, error) {
	// Make sure the WAL directory exists.
	if err := os.MkdirAll(dir, defaultDirPerm); err != nil {
		return nil, err
	}

	files, err := scanLogFiles(ctx, dir, cfg.blockSize)
	if err != nil {
		return nil, err
	}

	stream := &logStream{
		stream:    files,
		dir:       dir,
		blockSize: cfg.blockSize,
		fileSize:  cfg.fileSize,
		tracer:    tracing.NewTracer("store.wal.recovery", oteltracer.SpanKindInternal),
	}
	return stream, nil
}

func scanLogFiles(ctx context.Context, dir string, blockSize int64) (stream []*logFile, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files = filterRegularLog(files)

	// Rebuild log stream.
	var discards []*logFile
	var last *logFile
	for _, file := range files {
		filename := file.Name()
		so, err2 := strconv.ParseInt(filename[:len(filename)-len(logFileExt)], 10, 64)
		if err2 != nil {
			return nil, err2
		}

		if last != nil {
			// discontinuous log file
			if so != last.eo {
				log.Warning(ctx, "Discontinuous log file, discard before.",
					map[string]interface{}{
						"last_end":   last.eo,
						"next_start": so,
					})
				discards = append(discards, stream...)
				stream = nil
			}
		}

		info, err2 := file.Info()
		if err2 != nil {
			return nil, err2
		}

		path := filepath.Join(dir, filename)
		size := info.Size()

		if size%blockSize != 0 {
			// TODO(james.yin): return error
			truncated := size - size%blockSize
			log.Warning(context.Background(), "The size of log file is not a multiple of blockSize, truncate it.",
				map[string]interface{}{
					"file":        path,
					"origin_size": size,
					"new_size":    truncated,
				})
			size = truncated
		}

		last = newLogFile(path, so, size, nil)
		stream = append(stream, last)
	}

	// Delete discard files.
	for _, f := range discards {
		_ = os.Remove(f.path)
	}

	return stream, nil
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
