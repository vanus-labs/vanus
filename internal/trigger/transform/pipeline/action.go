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

package pipeline

import (
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
	"github.com/linkall-labs/vanus/internal/trigger/context"
	"github.com/pkg/errors"
)

type Action interface {
	Execute(ceCtx *context.EventContext) error
}

type action struct {
	args      []arg.Arg
	targetArg arg.Arg
	fn        function.Function
}

func newAction(command []interface{}) (*action, error) {
	funcName := command[0].(string)
	argNum := len(command) - 1
	fn, err := function.GetFunction(funcName, argNum)
	if err != nil {
		return nil, errors.Wrapf(err, "function %s error", funcName)
	}
	args := make([]arg.Arg, argNum)
	for i := 1; i < len(command); i++ {
		_arg, err := arg.NewArg(command[i])
		if err != nil {
			return nil, err
		}
		args[i-1] = _arg
	}
	return &action{
		args:      args,
		targetArg: args[fn.TargetArgIndex()],
		fn:        fn,
	}, nil
}

func (a *action) Execute(ceCtx *context.EventContext) error {
	funcName := a.fn.Name()
	switch funcName {
	case "DELETE":
		return a.targetArg.DeleteValue(ceCtx)
	}
	args := make([]interface{}, len(a.args))
	for i, _arg := range a.args {
		var v interface{}
		if a.fn.TargetArgIndex() == i && !a.fn.IsSourceTargetSame() {
			v = _arg.Original()
		} else {
			value, err := _arg.Evaluate(ceCtx)
			if err != nil {
				return err
			}
			if value == nil {
				return fmt.Errorf("arg %s value is nil", _arg.Original())
			}
			v, err = function.Cast(value, *a.fn.ArgType(i))
			if err != nil {
				return err
			}
		}
		args[i] = v
	}
	fnValue, err := a.fn.Execute(args)
	if err != nil {
		return err
	}
	if funcName == "CREATE" || a.fn.Name() == "MOVE" || a.fn.Name() == "RENAME" {
		v, _ := a.targetArg.Evaluate(ceCtx)
		if v != nil {
			return fmt.Errorf("key %s exist", a.targetArg.Original())
		}
	} else if funcName == "REPLACE" {
		v, _ := a.targetArg.Evaluate(ceCtx)
		if v == nil {
			return fmt.Errorf("key %s no exist", a.targetArg.Original())
		}
	}
	// set value to event
	if err = a.targetArg.SetValue(ceCtx, fnValue); err != nil {
		return err
	}
	// move(sourcePath,targetArg) delete original key
	if a.fn.Name() == "MOVE" || a.fn.Name() == "RENAME" {
		return a.args[0].DeleteValue(ceCtx)
	}
	return nil
}
