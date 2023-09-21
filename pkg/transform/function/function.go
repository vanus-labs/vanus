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

package function

import "github.com/vanus-labs/vanus/pkg/transform/common"

type Function interface {
	// Name func name
	Name() string
	// Arity arg number
	Arity() int
	// ArgType arg type
	ArgType(index int) *common.Type
	// IsVariadic is exist variadic
	IsVariadic() bool
	// Execute cal func result
	Execute(args []interface{}) (interface{}, error)
}

type function struct {
	name         string
	fixedArgs    []common.Type
	variadicArgs *common.Type
	fn           func(args []interface{}) (interface{}, error)
}

func (f function) Name() string {
	return f.name
}

func (f function) Arity() int {
	return len(f.fixedArgs)
}

func (f function) ArgType(index int) *common.Type {
	if index < len(f.fixedArgs) {
		return &f.fixedArgs[index]
	}
	return f.variadicArgs
}

func (f function) IsVariadic() bool {
	return f.variadicArgs != nil
}

func (f function) Execute(args []interface{}) (interface{}, error) {
	return f.fn(args)
}
