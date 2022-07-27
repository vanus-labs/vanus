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

package store

import (
	// standard libraries.
	"fmt"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/io"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
	"github.com/linkall-labs/vanus/pkg/util"
)

const (
	baseKB                         = 1024
	baseMB                         = 1024 * baseKB
	ioEnginePsync                  = "psync"
	baseWALBlockSize        uint64 = 4 * baseKB
	minMetaStoreWALFileSize uint64 = 4 * baseMB
	minRaftLogWALFileSize   uint64 = 32 * baseMB
	minWALFlushTimeout             = 200 * time.Microsecond
)

type Config struct {
	ControllerAddresses []string         `yaml:"controllers"`
	IP                  string           `yaml:"ip"`
	Port                int              `yaml:"port"`
	Volume              VolumeInfo       `yaml:"volume"`
	MetaStore           SyncStoreConfig  `yaml:"meta_store"`
	OffsetStore         AsyncStoreConfig `yaml:"offset_store"`
	Raft                RaftConfig       `yaml:"raft"`
}

func (c *Config) Validate() error {
	if err := c.MetaStore.validate(); err != nil {
		return err
	}
	if err := c.OffsetStore.validate(); err != nil {
		return err
	}
	if err := c.Raft.validate(); err != nil {
		return err
	}
	return nil
}

type VolumeInfo struct {
	ID       vanus.ID `json:"id"`
	Dir      string   `json:"dir"`
	Capacity uint64   `json:"capacity"`
}

type SyncStoreConfig struct {
	WAL WALConfig `yaml:"wal"`
}

func (c *SyncStoreConfig) validate() error {
	return c.WAL.validate(minMetaStoreWALFileSize)
}

type AsyncStoreConfig struct {
	WAL WALConfig `yaml:"wal"`
}

func (c *AsyncStoreConfig) validate() error {
	return c.WAL.validate(minMetaStoreWALFileSize)
}

type RaftConfig struct {
	WAL WALConfig `yaml:"wal"`
}

func (c *RaftConfig) validate() error {
	return c.WAL.validate(minRaftLogWALFileSize)
}

type WALConfig struct {
	BlockSize    uint64   `yaml:"block_size"`
	FileSize     uint64   `yaml:"file_size"`
	FlushTimeout string   `yaml:"flush_timeout"`
	IO           IOConfig `yaml:"io"`
}

func (c *WALConfig) validate(minFileSize uint64) error {
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

type IOConfig struct {
	Engine string `yaml:"engine"`
}

func (c *WALConfig) Options() (opts []walog.Option) {
	if c.BlockSize != 0 {
		opts = append(opts, walog.WithBlockSize(int64(c.BlockSize)))
	}
	if c.FileSize != 0 {
		opts = append(opts, walog.WithFileSize(int64(c.FileSize)))
	}
	if c.FlushTimeout != "" {
		d, _ := time.ParseDuration(c.FlushTimeout)
		opts = append(opts, walog.WithFlushTimeout(d))
	}
	if c.IO.Engine != "" {
		switch c.IO.Engine {
		case ioEnginePsync:
			opts = append(opts, walog.WithIOEngine(io.NewEngine()))
		default:
			opts = configWALIOEngineOptionEx(opts, c.IO)
		}
	}
	return opts
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	if err := primitive.LoadConfig(filename, c); err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = util.GetLocalIP()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}
