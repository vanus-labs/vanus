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

package file

import (
	// standard libraries.
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	errutil "github.com/linkall-labs/vanus/internal/util/errors"
	"github.com/linkall-labs/vanus/observability"
)

const (
	blockExt        = ".block"
	defaultFilePerm = 0o644
)

func resolvePath(blockDir string, id vanus.ID) string {
	return filepath.Join(blockDir, fmt.Sprintf("%020d%s", id.Uint64(), blockExt))
}

func Create(ctx context.Context, blockDir string, id vanus.ID, capacity int64) (*Block, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	path := resolvePath(blockDir, id)
	b := &Block{
		id:   id,
		path: path,
		cap:  capacity,
		actx: appendContext{
			offset: headerBlockSize,
		},
	}
	b.IDStr = strconv.FormatUint(id.Uint64(), 10)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_SYNC, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	if err = f.Truncate(capacity); err != nil {
		return nil, err
	}
	b.f = f

	if err = b.persistHeader(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func Open(ctx context.Context, path string) (*Block, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	filename := filepath.Base(path)
	id, err := vanus.NewIDFromString(filename[:len(filename)-len(blockExt)])
	if err != nil {
		return nil, err
	}

	b := &Block{
		id:   id,
		path: path,
	}

	// TODO: use direct IO
	f, err := os.OpenFile(path, os.O_RDWR|os.O_SYNC, defaultFilePerm)
	if err != nil {
		return nil, err
	}
	b.f = f

	// Recover block state.
	if err := b.recover(ctx); err != nil {
		b.f = nil
		if err2 := f.Close(); err2 != nil {
			return nil, errutil.Chain(err, err2)
		}
		return nil, err
	}

	return b, nil
}
