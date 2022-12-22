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
	"fmt"
	stdStrs "strings"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/pkg/errors"
)

type newAction func() action.Action

var actionMap = map[string]newAction{}

func AddAction(actionFn newAction) error {
	a := actionFn()
	name := stdStrs.ToUpper(a.Name())
	if _, exist := actionMap[name]; exist {
		return fmt.Errorf("action %s has exist", name)
	}
	actionMap[name] = actionFn
	return nil
}

func NewAction(command []interface{}) (action.Action, error) {
	funcName, ok := command[0].(string)
	if !ok {
		return nil, fmt.Errorf("command name must be string")
	}
	actionFn, exist := actionMap[stdStrs.ToUpper(funcName)]
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
