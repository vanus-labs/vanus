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

package array

import (
	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	"github.com/pkg/errors"
)

// ["foreach_array","array root", function].
type foreachArrayAction struct {
	action.CommonAction
	actions []action.Action
}

func NewForeachArrayAction() action.Action {
	a := &foreachArrayAction{}
	a.CommonAction = action.CommonAction{
		ActionName:  "FOREACH_ARRAY",
		FixedArgs:   []arg.TypeList{[]arg.Type{arg.EventData}, []arg.Type{arg.Constant}},
		VariadicArg: arg.TypeList{arg.Constant},
	}
	return a
}

func (a *foreachArrayAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args[:1]
	a.ArgTypes = []common.Type{common.Array}
	for i := 1; i < len(args); i++ {
		v, _ := args[i].Evaluate(nil)
		commands, ok := v.([]interface{})
		if !ok {
			return errors.Errorf("arg %d %s is invalid", i, args[i].Original())
		}
		_action, err := runtime.NewAction(commands)
		if err != nil {
			return errors.Wrapf(err, "arg %d %s new action error", i, args[i].Original())
		}
		a.actions = append(a.actions, _action)
	}
	return nil
}

func (a *foreachArrayAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	arrayValue, _ := args[0].([]interface{})
	for i := range arrayValue {
		newCtx := &context.EventContext{
			Data: arrayValue[i],
		}
		for i := range a.actions {
			err = a.actions[i].Execute(newCtx)
			if err != nil {
				return errors.Wrapf(err, "action %dst execute error", i+1)
			}
		}
	}
	return a.TargetArg.SetValue(ceCtx, arrayValue)
}
