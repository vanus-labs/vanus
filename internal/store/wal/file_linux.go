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

//go:build linux
// +build linux

package wal

import (
	// standard libraries.
	"os"
	"syscall"

	// third-party libraries.
	"github.com/ncw/directio"

	// this project.
	errutil "github.com/linkall-labs/vanus/internal/util/errors"
)

const FALLOC_FL_ZERO_RANGE uint32 = 0x10

func openFile(path string) (*os.File, error) {
	return directio.OpenFile(path, os.O_RDWR|syscall.O_DSYNC|syscall.O_NOATIME, 0)
}

func createFile(path string, size int64, wronly bool, sync bool) (*os.File, error) {
	flag := os.O_CREATE | os.O_EXCL | syscall.O_NOATIME
	if wronly {
		flag |= os.O_WRONLY
	} else {
		flag |= os.O_RDWR
	}
	if sync {
		flag |= syscall.O_DSYNC
	}
	f, err := directio.OpenFile(path, flag, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	// resize file
	if err = syscall.Fallocate(int(f.Fd()), FALLOC_FL_ZERO_RANGE, 0, size); err != nil {
		if err2 := f.Close(); err2 != nil {
			return f, errutil.Chain(err, err2)
		}
		return nil, err
	}
	return f, nil
}
