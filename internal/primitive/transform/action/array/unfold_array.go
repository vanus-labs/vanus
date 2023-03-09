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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
)

// ["unfold_array","sourceJsonPath", "targetPathPrefix"].
type unfoldArrayAction struct {
	action.CommonAction
	targetPathPrefix string
}

func NewUnfoldArrayAction() action.Action {
	a := &unfoldArrayAction{}
	a.CommonAction = action.CommonAction{
		ActionName: "UNFOLD_ARRAY",
		FixedArgs:  []arg.TypeList{[]arg.Type{arg.EventData}, []arg.Type{arg.EventData}},
	}
	return a
}

func (a *unfoldArrayAction) Init(args []arg.Arg) error {
	a.targetPathPrefix = args[1].Original()
	a.Args = args[:1]
	a.ArgTypes = []common.Type{common.Array}
	return nil
}

func (a *unfoldArrayAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	arrayValue, _ := args[0].([]interface{})
	errs := make([]string, 0, len(arrayValue))
	for i, v := range arrayValue {
		argument := a.targetPathPrefix + "-" + strconv.Itoa(i)
		targetArg, err := arg.NewArg(argument)
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to NewArg, arg: %v, err: %v", argument, err))
			continue
		}
		err = targetArg.SetValue(ceCtx, v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("failed to SetValue, v: %v, err: %v", v, err))
			continue
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ";"))
	}
	return nil
}
