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

package action

import (
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
	"github.com/pkg/errors"
)

type Action interface {
	// Name func name
	Name() string
	// Arity arg number
	Arity() int
	// ArgType arg type
	ArgType(index int) arg.TypeList
	// IsVariadic is exist variadic
	IsVariadic() bool
	Init(args []arg.Arg) error
	Execute(ceCtx *context.EventContext) error
}

type CommonAction struct {
	ActionName  string
	FixedArgs   []arg.TypeList
	VariadicArg arg.TypeList
	Fn          function.Function

	Args      []arg.Arg
	ArgTypes  []common.Type
	TargetArg arg.Arg
}

func (a *CommonAction) Name() string {
	return a.ActionName
}

func (a *CommonAction) Arity() int {
	return len(a.FixedArgs)
}

func (a *CommonAction) ArgType(index int) arg.TypeList {
	if index < len(a.FixedArgs) {
		return a.FixedArgs[index]
	}
	return a.VariadicArg
}

func (a *CommonAction) IsVariadic() bool {
	return len(a.VariadicArg) > 0
}

func (a *CommonAction) RunArgs(ceCtx *context.EventContext) ([]interface{}, error) {
	args := make([]interface{}, len(a.Args))
	if len(a.Args) != len(a.ArgTypes) {
		return nil, fmt.Errorf("arg lenth %d not same arg type %d", len(a.Args), len(a.ArgTypes))
	}
	for i, _arg := range a.Args {
		value, err := _arg.Evaluate(ceCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "arg  %s evaluate error", _arg.Original())
		}
		v, err := common.Cast(value, a.ArgTypes[i])
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	return args, nil
}

type FunctionAction struct {
	CommonAction
}

func (a *FunctionAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args[1:]
	return a.setArgTypes()
}

func (a *FunctionAction) setArgTypes() error {
	if a.Fn == nil {
		return fmt.Errorf("fn is nil")
	}
	if len(a.Args) < a.Fn.Arity() {
		return ErrArgNumber
	}
	if len(a.Args) > a.Fn.Arity() && !a.Fn.IsVariadic() {
		return ErrArgNumber
	}
	argTypes := make([]common.Type, len(a.Args))
	for i := 0; i < len(a.Args); i++ {
		argTypes[i] = *a.Fn.ArgType(i)
	}
	a.ArgTypes = argTypes
	return nil
}

func (a *FunctionAction) Execute(ceCtx *context.EventContext) error {
	if a.Fn == nil {
		return fmt.Errorf("fn is nil")
	}
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	fnValue, err := a.Fn.Execute(args)
	if err != nil {
		return err
	}
	return a.TargetArg.SetValue(ceCtx, fnValue)
}

type SourceTargetSameAction struct {
	FunctionAction
}

func (a *SourceTargetSameAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args
	return a.setArgTypes()
}

var (
	ErrExist     = fmt.Errorf("action have exist")
	ErrArgNumber = fmt.Errorf("action arg number invalid")
)
