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

//go:generate mockgen -source=block.go  -destination=testing/mock_block.go -package=testing
package block

import (
	// standard libraries.
	"context"
	"errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

var (
	ErrNotEnoughSpace = errors.New("not enough space")
	ErrFull           = errors.New("full")
	ErrNotLeader      = errors.New("not leader")
	ErrExceeded       = errors.New("the offset exceeded")
	ErrOnEnd          = errors.New("the offset on end")
)

type Reader interface {
	Read(ctx context.Context, seq int64, num int) ([]Entry, error)
}

type Appender interface {
	Append(ctx context.Context, entries ...Entry) ([]int64, error)
}

type Block interface {
	Reader
	Appender

	ID() vanus.ID
}
