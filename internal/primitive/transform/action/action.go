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
	"strings"

	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
	"github.com/pkg/errors"
)

type newAction func() Action

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

type commonAction struct {
	name        string
	fixedArgs   []arg.TypeList
	variadicArg arg.TypeList
	fn          function.Function

	args      []arg.Arg
	argTypes  []function.Type
	targetArg arg.Arg
}

func (a *commonAction) Name() string {
	if a.name != "" {
		return a.name
	}
	if a.fn != nil {
		return a.fn.Name()
	}
	return ""
}

func (a *commonAction) Arity() int {
	return len(a.fixedArgs)
}

func (a *commonAction) ArgType(index int) arg.TypeList {
	if index < len(a.fixedArgs) {
		return a.fixedArgs[index]
	}
	return a.variadicArg
}

func (a *commonAction) IsVariadic() bool {
	return len(a.variadicArg) > 0
}

func (a *commonAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args[1:]
	return a.setArgTypes()
}

func (a *commonAction) setArgTypes() error {
	if a.fn == nil {
		return fmt.Errorf("fn is nil")
	}
	if len(a.args) < a.fn.Arity() {
		return ErrArgNumber
	}
	if len(a.args) > a.fn.Arity() && !a.fn.IsVariadic() {
		return ErrArgNumber
	}
	argTypes := make([]function.Type, len(a.args))
	for i := 0; i < len(a.args); i++ {
		argTypes[i] = *a.fn.ArgType(i)
	}
	a.argTypes = argTypes
	return nil
}

func (a *commonAction) Execute(ceCtx *context.EventContext) error {
	if a.fn == nil {
		return fmt.Errorf("fn is nil")
	}
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	fnValue, err := a.fn.Execute(args)
	if err != nil {
		return err
	}
	return a.targetArg.SetValue(ceCtx, fnValue)
}

func (a *commonAction) runArgs(ceCtx *context.EventContext) ([]interface{}, error) {
	args := make([]interface{}, len(a.args))
	if len(a.args) != len(a.argTypes) {
		return nil, fmt.Errorf("arg lenth %d not same arg type %d", len(a.args), len(a.argTypes))
	}
	for i, _arg := range a.args {
		value, err := _arg.Evaluate(ceCtx)
		if err != nil {
			return nil, errors.Wrapf(err, "arg  %s evaluate error", _arg.Original())
		}
		v, err := function.Cast(value, a.argTypes[i])
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	return args, nil
}

type sourceTargetSameAction struct {
	commonAction
}

func (a *sourceTargetSameAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args
	return a.setArgTypes()
}

var actionMap = map[string]newAction{}

func AddAction(actionFn newAction) error {
	a := actionFn()
	if _, exist := actionMap[a.Name()]; exist {
		return ErrExist
	}
	actionMap[a.Name()] = actionFn
	return nil
}

func init() {
	for _, fn := range []newAction{
		// struct
		newCreateActionAction,
		newDeleteAction,
		newReplaceAction,
		newMoveActionAction,
		newRenameActionAction,
		// math
		newMathAddActionAction,
		newMathSubActionAction,
		newMathMulActionAction,
		newMathDivActionAction,
		// format
		newDateFormatAction,
		newUnixTimeFormatAction,
		// string
		newJoinAction,
		newUpperAction,
		newLowerAction,
		newAddPrefixAction,
		newAddSuffixAction,
		newReplaceWithRegexAction,
	} {
		if err := AddAction(fn); err != nil {
			panic(err)
		}
	}
}

func NewAction(command []interface{}) (Action, error) {
	funcName, ok := command[0].(string)
	if !ok {
		return nil, fmt.Errorf("command name must be stirng")
	}
	actionFn, exist := actionMap[strings.ToUpper(funcName)]
	if !exist {
		return nil, fmt.Errorf("command %s not exist", funcName)
	}
	a := actionFn()
	argNum := len(command) - 1
	if argNum < a.Arity() {
		return nil, fmt.Errorf("command %s arg number is not enough, it need %d but only have %d",
			funcName, a.Arity(), argNum)
	}
	if argNum > a.Arity() && !a.IsVariadic() {
		return nil, fmt.Errorf("command %s arg number is too many, it need %d but have %d", funcName, a.Arity(), argNum)
	}
	args := make([]arg.Arg, argNum)
	for i := 1; i < len(command); i++ {
		_arg, err := arg.NewArg(command[i])
		if err != nil {
			return nil, errors.Wrapf(err, "command %s arg %d is invalid", funcName, i)
		}
		argType := a.ArgType(i - 1)
		if !argType.Contains(_arg) {
			return nil, fmt.Errorf("command %s arg %d not support type %s", funcName, i, _arg.Type())
		}
		args[i-1] = _arg
	}
	err := a.Init(args)
	if err != nil {
		return nil, errors.Wrapf(err, "command %s init error", funcName)
	}
	return a, nil
}

var (
	ErrExist     = fmt.Errorf("action have exist")
	ErrArgNumber = fmt.Errorf("action arg number invalid")
)
