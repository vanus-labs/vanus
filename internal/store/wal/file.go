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
	"fmt"
	"io"
	"os"
	"path/filepath"

	// this project.
	errutil "github.com/linkall-labs/vanus/internal/util/errors"
)

const (
	defaultFilePerm = 0644
	logFileExt      = ".log"
)

type logFile struct {
	so   int64
	size int64
	path string
	f    *os.File
}

// Make sure logFile implements io.WriteAt.
var _ io.WriterAt = (*logFile)(nil)

func (l *logFile) Close() error {
	if l.f == nil {
		return nil
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	l.f = nil
	return nil
}

func (l *logFile) Open() error {
	if l.f != nil {
		return nil
	}
	f, err := os.OpenFile(l.path, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		return err
	}
	l.f = f
	return nil
}

func (l *logFile) WriteAt(p []byte, off int64) (int, error) {
	if off < l.so {
		panic("underflow")
	}
	if off+int64(len(p)) > l.so+l.size {
		panic("overflow")
	}
	return l.f.WriteAt(p, off-l.so)
}

func createLogFile(dir string, so, size int64, sync bool) (*logFile, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d%s", so, logFileExt))
	f, err := createFile(path, size, true, sync)
	if err != nil {
		return nil, err
	}
	return &logFile{
		so:   so,
		size: size,
		path: path,
		f:    f,
	}, nil
}

func createFile(path string, size int64, wronly bool, sync bool) (*os.File, error) {
	flag := os.O_CREATE | os.O_EXCL
	if wronly {
		flag |= os.O_WRONLY
	} else {
		flag |= os.O_RDWR
	}
	if sync {
		flag |= os.O_SYNC
	}
	f, err := os.OpenFile(path, flag, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	// resize file
	if err = f.Truncate(size); err != nil {
		if err2 := f.Close(); err2 != nil {
			return f, errutil.Chain(err, err2)
		}
		return nil, err
	}
	return f, nil
}
