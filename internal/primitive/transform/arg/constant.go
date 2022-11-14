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

package arg

import (
	"fmt"

	"github.com/linkall-labs/vanus/internal/trigger/context"
)

type constant struct {
	value interface{}
}

func newConstant(value interface{}) Arg {
	return constant{
		value: value,
	}
}

func (arg constant) Type() Type {
	return Constant
}
func (arg constant) Name() string {
	return arg.Original()
}
func (arg constant) Original() string {
	switch arg.value.(type) {
	case string:
		return arg.value.(string)
	}
	return fmt.Sprintf("%v", arg.value)
}

func (arg constant) Evaluate(*context.EventContext) (interface{}, error) {
	return arg.value, nil
}

func (arg constant) SetValue(*context.EventContext, interface{}) error {
	return ErrOperationNotSupport
}

func (arg constant) DeleteValue(ceCtx *context.EventContext) error {
	return ErrOperationNotSupport
}
