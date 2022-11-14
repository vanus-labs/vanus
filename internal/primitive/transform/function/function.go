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

import (
	"fmt"
	"strings"
)

type Function interface {
	// Name func name
	Name() string
	// Arity arg number
	Arity() int
	// ArgType arg type
	ArgType(index int) *Type
	// IsVariadic is exist variadic
	IsVariadic() bool
	// TargetArgIndex target path arg in arg index
	TargetArgIndex() int
	// IsSourceTargetSame source path is also target path
	IsSourceTargetSame() bool
	// Execute cal func result
	Execute(args []interface{}) (interface{}, error)
}

type function struct {
	name             string
	fixedArgs        []Type
	variadicArgs     *Type
	targetArgIndex   int
	sourceTargetSame bool
	fn               func(args []interface{}) (interface{}, error)
}

func (f function) Name() string {
	return f.name
}

func (f function) Arity() int {
	return len(f.fixedArgs)
}

func (f function) ArgType(index int) *Type {
	if index < len(f.fixedArgs) {
		return &f.fixedArgs[index]
	}
	return f.variadicArgs
}

func (f function) IsVariadic() bool {
	return f.variadicArgs != nil
}

func (f function) TargetArgIndex() int {
	return f.targetArgIndex
}

func (f function) IsSourceTargetSame() bool {
	return f.sourceTargetSame
}

func (f function) Execute(args []interface{}) (interface{}, error) {
	return f.fn(args)
}

var funcMap map[string]Function

func AddFunction(fn Function) {
	funcMap[fn.Name()] = fn
}

func GetFunction(name string, args int) (Function, error) {
	f, exist := funcMap[strings.ToUpper(name)]
	if !exist {
		return nil, ErrNoExist
	}
	if args < f.Arity() {
		return nil, ErrArgNumber
	}
	if f.Arity() != args && !f.IsVariadic() {
		return nil, ErrArgNumber
	}
	return f, nil
}

func init() {
	funcMap = make(map[string]Function)
	AddFunction(joinFunction)
}

var (
	ErrNoExist   = fmt.Errorf("function no exist")
	ErrArgNumber = fmt.Errorf("function arg number invalid")
)
