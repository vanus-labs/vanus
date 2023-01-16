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

package runtime

import (
	"strings"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/pkg/errors"
)

type newAction func() action.Action

var actionMap = map[string]newAction{}

func AddAction(actionFn newAction) error {
	a := actionFn()
	name := strings.ToUpper(a.Name())
	if _, exist := actionMap[name]; exist {
		return errors.Errorf("action %s has exist", name)
	}
	actionMap[name] = actionFn
	return nil
}

func NewAction(command []interface{}) (action.Action, error) {
	funcName, ok := command[0].(string)
	if !ok {
		return nil, errors.Errorf("command name must be string")
	}
	actionFn, exist := actionMap[strings.ToUpper(funcName)]
	if !exist {
		return nil, errors.Errorf("command %s not exist", funcName)
	}
	a := actionFn()
	argNum := len(command) - 1
	if argNum < a.Arity() {
		return nil, errors.Errorf("command %s arg number is not enough, it need %d but only have %d",
			funcName, a.Arity(), argNum)
	}
	nestAction, isNestAction := a.(action.NestAction)
	if !isNestAction {
		if argNum > a.Arity() && !a.IsVariadic() {
			return nil, errors.Errorf("command %s arg number is too many, it need %d but have %d", funcName, a.Arity(), argNum)
		}
	} else {
		argNum = a.Arity()
	}
	args := make([]arg.Arg, argNum)
	for i := 0; i < len(args); i++ {
		index := i + 1
		_arg, err := arg.NewArg(command[index])
		if err != nil {
			return nil, errors.Wrapf(err, "command %s arg %d is invalid", funcName, index)
		}
		argType := a.ArgType(i)
		if !argType.Contains(_arg) {
			return nil, errors.Errorf("command %s arg %d not support type %s", funcName, index, _arg.Type())
		}
		args[i] = _arg
	}
	err := a.Init(args)
	if err != nil {
		return nil, errors.Wrapf(err, "command %s init error", funcName)
	}
	if isNestAction {
		actions := make([]action.Action, len(command)-1-argNum)
		if len(actions) == 0 {
			return nil, errors.Errorf("command %s arg number is not enough, lost function arg", funcName)
		}
		for i := 0; i < len(actions); i++ {
			index := i + 1 + argNum
			if arr, ok := command[index].([]interface{}); ok {
				_a, err := NewAction(arr)
				if err != nil {
					return nil, errors.Wrapf(err, "action %s arg %d new action failed", funcName, index)
				}
				actions[i] = _a
			} else {
				return nil, errors.Errorf("arg %d is invalid", index)
			}
		}
		err = nestAction.InitAction(actions)
		if err != nil {
			return nil, errors.Wrapf(err, "command %s init action error", funcName)
		}
	}
	return a, nil
}
