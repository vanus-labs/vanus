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

package io

import (
	// standard libraries.
	"os"

	// third-party libraries.
	"github.com/ncw/directio"

	// first-party libraries.
	"github.com/linkall-labs/vanus/pkg/errors"
)

const (
	defaultFilePerm = 0o644
	baseFillSize    = 4 * 1024 // 4KB
	fastFillVecSize = 11       // Up to 4MB
)

var fastFillVec []fastFillTemplate

func init() { //nolint:gochecknoinits // Initialize fastFileVec.
	fastFillVec = make([]fastFillTemplate, fastFillVecSize)

	n := fastFillVecSize - 1
	for i := 0; i <= n; i++ {
		size := (1 << (n - i)) * baseFillSize
		fastFillVec[i] = fastFillTemplate{
			size: int64(size),
			data: directio.AlignedBlock(size),
		}
	}
}

type fastFillTemplate struct {
	size int64
	data []byte
}

func OpenFile(path string, flag int, sync bool, direct bool) (*os.File, error) {
	return openFile(path, openFileFlag|flag, sync, direct)
}

func CreateFile(path string, size int64, flag int, sync bool, direct bool) (*os.File, error) {
	return createFile(path, size, flag, sync, direct)
}

func ResizeFile(f *os.File, size int64) error {
	return resizeFile(f, size)
}

func doCreateFile(path string, size int64, flag int, sync bool, direct bool) (*os.File, error) {
	f, err := openFile(path, createFileFlag|flag, sync, direct)
	if err != nil {
		return nil, err
	}

	if size <= 0 {
		return f, nil
	}

	// Resize file.
	if err = resizeFile(f, size); err != nil {
		if err2 := f.Close(); err2 != nil {
			return f, errors.Chain(err, err2)
		}
		return nil, err
	}

	return f, nil
}

func doCreateFileAndWarm( //nolint:unused,nolintlint // use in linux.
	path string, size int64, flag int, sync bool, direct bool,
) (*os.File, error) {
	// Create file.
	f, err := openFile(path, createFileFlag|os.O_WRONLY, false, true)
	if err != nil {
		return nil, err
	}

	// Resize file.
	if err = resizeFile(f, size); err != nil {
		if err2 := f.Close(); err2 != nil {
			return f, errors.Chain(err, err2)
		}
		return nil, err
	}

	// Warm file.
	if err = warmFile(f, size); err != nil {
		if err2 := f.Close(); err2 != nil {
			return f, errors.Chain(err, err2)
		}
		return nil, err
	}

	if err = f.Close(); err != nil {
		return f, err
	}

	f, err = openFile(path, openFileFlag|flag, sync, direct)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func openFile(path string, flag int, sync bool, direct bool) (*os.File, error) {
	if direct {
		return directio.OpenFile(path, makeFlag(flag, sync), defaultFilePerm)
	}
	return os.OpenFile(path, makeFlag(flag, sync), defaultFilePerm)
}

func makeFlag(flag int, sync bool) int {
	if sync {
		flag |= syncFlag
	}
	return flag
}

func warmFile(f *os.File, size int64) error { //nolint:unused,nolintlint // use in linux.
	for i, off := 0, int64(0); i < len(fastFillVec); i++ {
		for fill := &fastFillVec[i]; off+fill.size <= size; off += fill.size {
			if _, err := f.WriteAt(fill.data, off); err != nil {
				return err
			}
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}
