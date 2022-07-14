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
	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/io"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
	"github.com/linkall-labs/vanus/internal/util"
)

const ioEnginePsync = "psync"

type Config struct {
	ControllerAddresses []string         `yaml:"controllers"`
	IP                  string           `yaml:"ip"`
	Port                int              `yaml:"port"`
	Volume              VolumeInfo       `yaml:"volume"`
	MetaStore           SyncStoreConfig  `yaml:"meta_store"`
	OffsetStore         AsyncStoreConfig `yaml:"offset_store"`
	Raft                RaftConfig       `yaml:"raft"`
}

type VolumeInfo struct {
	ID       vanus.ID `json:"id"`
	Dir      string   `json:"dir"`
	Capacity uint64   `json:"capacity"`
}

type SyncStoreConfig struct {
	WAL WALConfig `yaml:"wal"`
}

type AsyncStoreConfig struct {
	WAL WALConfig `yaml:"wal"`
}

type RaftConfig struct {
	WAL WALConfig `yaml:"wal"`
}

type WALConfig struct {
	FileSize int64    `yaml:"file_size"`
	IO       IOConfig `yaml:"io"`
}

type IOConfig struct {
	Engine string `yaml:"engine"`
}

func (c *WALConfig) Options() (opts []walog.Option) {
	if c.FileSize > 0 {
		opts = append(opts, walog.WithFileSize(c.FileSize))
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
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = util.GetLocalIP()
	}
	return c, nil
}
