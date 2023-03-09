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

package strings

import (
	"strings"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
)

type replaceStringAction struct {
	action.CommonAction
}

// NewReplaceStringAction ["path", "subValue", "targetValue"].
func NewReplaceStringAction() action.Action {
	return &replaceStringAction{
		CommonAction: action.CommonAction{
			ActionName: "REPLACE_STRING",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All},
		},
	}
}

func (a *replaceStringAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args
	a.ArgTypes = []common.Type{common.String, common.String, common.String}
	return nil
}

func (a *replaceStringAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	originalString, _ := args[0].(string)
	subVal, _ := args[1].(string)
	targetVal, _ := args[2].(string)
	newString := strings.ReplaceAll(originalString, subVal, targetVal)
	return a.TargetArg.SetValue(ceCtx, newString)
}
