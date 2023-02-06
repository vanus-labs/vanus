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
	// this project.
	"github.com/linkall-labs/vanus/internal/store/vsb"
)

type VSB struct {
	FlushBatchSize int `yaml:"flush_batch_size"`
	IO             `yaml:"io"`
}

func (c *VSB) Validate() error {
	return nil
}

func (c *VSB) Options() (opts []vsb.Option) {
	if c.FlushBatchSize != 0 {
		opts = append(opts, vsb.WithFlushBatchSize(c.FlushBatchSize))
	}
	if c.IO.Engine != "" {
		opts = append(opts, vsb.WithIOEngine(buildIOEngine(c.IO)))
	}
	return opts
}
