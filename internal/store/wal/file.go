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
	"os"
	"path/filepath"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
)

const logFileExt = ".log"

type logFile struct {
	// so is the start offset of the log file.
	so int64
	// eo is the end offset of the log file.
	eo int64

	size int64
	path string

	f *os.File
}

func newLogFile(path string, so int64, size int64, f *os.File) *logFile {
	return &logFile{
		so:   so,
		eo:   so + size,
		size: size,
		path: path,
		f:    f,
	}
}

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

func (l *logFile) Open(wronly bool) error {
	if l.f != nil {
		return nil
	}
	f, err := io.OpenFile(l.path, wronly, true)
	if err != nil {
		return err
	}
	l.f = f
	return nil
}

func (l *logFile) WriteAt(e io.Engine, b []byte, off int64, cb io.WriteCallback) {
	if off < l.so {
		panic("underflow")
	}
	if off+int64(len(b)) > l.eo {
		panic("overflow")
	}

	e.WriteAt(l.f, b, off-l.so, cb)
}

func createLogFile(dir string, so, size int64, sync bool) (*logFile, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d%s", so, logFileExt))
	f, err := io.CreateFile(path, size, true, sync)
	if err != nil {
		return nil, err
	}
	return newLogFile(path, so, size, f), nil
}
