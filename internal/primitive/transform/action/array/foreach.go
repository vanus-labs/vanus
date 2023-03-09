// Copyright 2023 Linkall Inc.
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
	"github.com/pkg/errors"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
)

// ["array_foreach","array root", function].
type arrayForeachAction struct {
	action.NestActionImpl
}

func NewArrayForeachAction() action.Action {
	a := &arrayForeachAction{}
	a.CommonAction = action.CommonAction{
		ActionName: "ARRAY_FOREACH",
		FixedArgs:  []arg.TypeList{[]arg.Type{arg.EventData}},
	}
	return a
}

func (a *arrayForeachAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args
	a.ArgTypes = []common.Type{common.Array}
	return nil
}

func (a *arrayForeachAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	arrayValue, _ := args[0].([]interface{})
	for i := range arrayValue {
		newCtx := &context.EventContext{
			Data: arrayValue[i],
		}
		for i := range a.Actions {
			err = a.Actions[i].Execute(newCtx)
			if err != nil {
				return errors.Wrapf(err, "action %dst execute error", i+1)
			}
		}
	}
	return a.TargetArg.SetValue(ceCtx, arrayValue)
}
