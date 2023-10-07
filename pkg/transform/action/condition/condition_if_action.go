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

package condition

import (
	"github.com/pkg/errors"

	"github.com/vanus-labs/vanus/pkg/transform/action"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/common"
	"github.com/vanus-labs/vanus/pkg/transform/context"
)

type conditionIfAction struct {
	action.CommonAction
}

// NewConditionIfAction ["condition_if","$.targetPath","$.path","op","compareValue","trueValue","falseValue"]
// op must be string and only support ==,>=,>,<=,< .
func NewConditionIfAction() action.Action {
	return &conditionIfAction{
		action.CommonAction{
			ActionName: "CONDITION_IF",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All, arg.All, arg.All, arg.All},
		},
	}
}

func (a *conditionIfAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args[1:]
	return nil
}

func (a *conditionIfAction) Execute(ceCtx *context.EventContext) error {
	v, err := a.Args[1].Evaluate(ceCtx)
	if err != nil {
		return errors.Wrapf(err, "arg %s evaluate error", a.Args[1].Original())
	}
	op, ok := v.(string)
	if !ok {
		return errors.New("op type must be string")
	}
	if op == "==" {
		a.ArgTypes = []common.Type{common.String, common.String, common.String, common.Any, common.Any}
	} else {
		switch op {
		case ">=", ">", "<=", "<":
			a.ArgTypes = []common.Type{common.Float, common.String, common.Float, common.Any, common.Any}
		default:
			return errors.Errorf("not support op [%s]", op)
		}
	}
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	var result bool
	switch op {
	case "==":
		result = args[0] == args[2]
	case ">=":
		result = args[0].(float64) >= args[2].(float64)
	case ">":
		result = args[0].(float64) > args[2].(float64)
	case "<=":
		result = args[0].(float64) <= args[2].(float64)
	case "<":
		result = args[0].(float64) < args[2].(float64)
	}
	if result {
		return a.TargetArg.SetValue(ceCtx, args[3])
	}
	return a.TargetArg.SetValue(ceCtx, args[4])
}
