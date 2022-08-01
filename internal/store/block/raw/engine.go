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

package raw

import (
	// standard libraries.
	"context"
	"fmt"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
)

const (
	VSB = "vsb"
)

var (
	ErrFormatRegistered = fmt.Errorf("format already registered")
	ErrNotSupported     = fmt.Errorf("not supported format")
	ErrInvalidFormat    = fmt.Errorf("invalid format")
)

type Engine interface {
	Recover(ctx context.Context) (map[vanus.ID]block.Raw, error)

	Create(ctx context.Context, id vanus.ID, capacity int64) (block.Raw, error)
	// Open(ctx context.Context, id vanus.ID) (block.Raw, error)

	GetBlockStatistics(id vanus.ID, block block.Raw) (block.Statistics, error)
}

var engines = map[string]Engine{}

func RegisterEngine(name string, engine Engine) error {
	if _, ok := engines[name]; ok {
		return ErrFormatRegistered
	}
	engines[name] = engine
	return nil
}

func ResolveEngine(engine string) (Engine, error) {
	if e, ok := engines[engine]; ok {
		return e, nil
	}
	return nil, ErrNotSupported
}
