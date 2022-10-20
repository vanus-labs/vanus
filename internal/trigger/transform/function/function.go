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

import "strings"

type Function interface {
	// Name func name
	Name() string
	// Arity arg number
	Arity() int
	// ArgType arg type
	ArgType(index int) *Type
	// TargetArgIndex target path arg in arg index
	TargetArgIndex() int
	// IsSourceTarget tart path is also source
	IsSourceTarget() bool
	// Execute cal func result
	Execute(args []interface{}) (interface{}, error)
}

type function struct {
	name           string
	fixedArgs      []Type
	targetArgIndex int
	sourceTarget   bool
	fn             func(args []interface{}) (interface{}, error)
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
	return nil
}

func (f function) TargetArgIndex() int {
	return f.targetArgIndex
}

func (f function) IsSourceTarget() bool {
	return f.sourceTarget
}

func (f function) Execute(args []interface{}) (interface{}, error) {
	return f.fn(args)
}

var funcMap map[string]Function

func AddFunction(fn Function) {
	funcMap[fn.Name()] = fn
}

func GetFunction(name string) Function {
	f, exist := funcMap[strings.ToUpper(name)]
	if !exist {
		return nil
	}
	return f
}

func init() {
	funcMap = make(map[string]Function)
	AddFunction(mergeFunction)
}
