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

package io

import (
	// standard libraries.
	"os"
	"syscall"
)

const (
	openFileFlag   = syscall.O_NOATIME
	createFileFlag = os.O_CREATE | os.O_EXCL | syscall.O_NOATIME
	syncFlag       = syscall.O_DSYNC
)

func createFile(path string, size int64, flag int, sync bool, direct bool) (*os.File, error) {
	if size > 0 && sync {
		return doCreateFileAndWarm(path, size, flag, sync, direct)
	}
	return doCreateFile(path, size, flag, sync, direct)
}

func resizeFile(f *os.File, size int64) error {
	return syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
