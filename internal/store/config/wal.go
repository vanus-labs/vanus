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

package config

import (
	// standard libraries.
	"fmt"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	baseKB             = 1024
	baseMB             = 1024 * baseKB
	baseWALBlockSize   = 4 * baseKB
	minWALFlushTimeout = 200 * time.Microsecond
)

type WAL struct {
	BlockSize    int    `yaml:"block_size"`
	FileSize     uint64 `yaml:"file_size"`
	FlushTimeout string `yaml:"flush_timeout"`
	IO           `yaml:"io"`
}

func (c *WAL) Validate(minFileSize uint64) error {
	if c.BlockSize != 0 && c.BlockSize%baseWALBlockSize != 0 {
		return fmt.Errorf("wal block size must be a multiple of %dKB", baseWALBlockSize/baseKB)
	}
	if c.FileSize != 0 && c.FileSize < minFileSize {
		return fmt.Errorf("wal file size must not less than %dMB", minFileSize/baseMB)
	}
	if c.FlushTimeout != "" {
		d, err := time.ParseDuration(c.FlushTimeout)
		if err != nil {
			return err
		}
		if d < minWALFlushTimeout {
			return fmt.Errorf("wal flush timeout must not less than %v", minWALFlushTimeout)
		}
	}
	return nil
}

func (c *WAL) Options() (opts []wal.Option) {
	if c.BlockSize != 0 {
		opts = append(opts, wal.WithBlockSize(c.BlockSize))
	}
	if c.FileSize != 0 {
		opts = append(opts, wal.WithFileSize(int64(c.FileSize)))
	}
	if c.FlushTimeout != "" {
		d, _ := time.ParseDuration(c.FlushTimeout)
		opts = append(opts, wal.WithFlushTimeout(d))
	}
	if c.IO.Engine != "" {
		opts = append(opts, wal.WithIOEngine(buildIOEngine(c.IO)))
	}
	return opts
}
