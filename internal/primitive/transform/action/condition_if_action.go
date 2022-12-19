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
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/pkg/errors"
)

type conditionIfAction struct {
	commonAction
}

// ["condition_if","$.targetPath","$.path","op","compareValue","trueValue","falseValue"]
// op must be string and only support ==,>=,>,<=,< .
func newConditionIfAction() Action {
	return &conditionIfAction{
		commonAction{
			name:      "CONDITION_IF",
			fixedArgs: []arg.TypeList{arg.EventList, arg.All, arg.All, arg.All, arg.All, arg.All},
		},
	}
}

func (a *conditionIfAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args[1:]
	return nil
}

func (a *conditionIfAction) Execute(ceCtx *context.EventContext) error {
	v, err := a.args[1].Evaluate(ceCtx)
	if err != nil {
		return errors.Wrapf(err, "arg %s evaluate error", a.args[1].Original())
	}
	op, ok := v.(string)
	if !ok {
		return errors.New("op type must be string")
	}
	if op == "==" {
		a.argTypes = []common.Type{common.String, common.String, common.String, common.Any, common.Any}
	} else {
		switch op {
		case ">=", ">", "<=", "<":
			a.argTypes = []common.Type{common.Number, common.String, common.Number, common.Any, common.Any}
		default:
			return errors.Errorf("not support op [%s]", op)
		}
	}
	args, err := a.runArgs(ceCtx)
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
		return a.targetArg.SetValue(ceCtx, args[3])
	}
	return a.targetArg.SetValue(ceCtx, args[4])
}
