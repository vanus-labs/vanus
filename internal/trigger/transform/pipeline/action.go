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

	"github.com/linkall-labs/vanus/internal/trigger/context"
	"github.com/linkall-labs/vanus/internal/trigger/transform/arg"
	"github.com/linkall-labs/vanus/internal/trigger/transform/function"
)

type action struct {
	args       []arg.Arg
	targetPath arg.Arg
	fn         function.Function
}

func newAction(command []interface{}) (*action, error) {
	funcName := command[0].(string)
	fn := function.GetFunction(funcName)
	if fn == nil {
		return nil, fmt.Errorf("function %s not exist", funcName)
	}
	if fn.Arity() > len(command)-1 {
		return nil, fmt.Errorf("function %s has %d args,but now is %d", funcName, fn.Arity(), len(command)-1)
	}
	args := make([]arg.Arg, len(command)-1)
	for i := 1; i < len(command); i++ {
		_arg, err := arg.NewArg(command[i])
		if err != nil {
			return nil, err
		}
		args[i-1] = _arg
	}
	return &action{
		args:       args,
		targetPath: args[fn.TargetArgIndex()],
		fn:         fn,
	}, nil
}

func (a *action) Execute(ceCtx *context.EventContext) error {
	if a.fn.Name() == "DELETE" {
		if a.targetPath.Type() == arg.EventAttribute {
			err := DeleteAttribute(ceCtx.Event, a.targetPath.Name())
			if err != nil {
				return err
			}
		} else if a.targetPath.Type() == arg.EventData {
			err := DeleteData(ceCtx.Data, a.targetPath.Name())
			if err != nil {
				return err
			}
		}
		return nil
	}
	args := make([]interface{}, len(a.args))
	for i, _arg := range a.args {
		var v interface{}
		if a.fn.TargetArgIndex() == i && !a.fn.IsSourceTarget() {
			v = _arg.Name()
		} else {
			value, err := _arg.Evaluate(ceCtx)
			if err != nil {
				return err
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
	// write value to data
	if a.targetPath.Type() == arg.EventAttribute {
		err = SetAttribute(ceCtx.Event, a.targetPath.Name(), fnValue)
		if err != nil {
			return err
		}
	} else if a.targetPath.Type() == arg.EventData {
		SetData(ceCtx.Data, a.targetPath.Name(), fnValue)
	}
	return nil
}
