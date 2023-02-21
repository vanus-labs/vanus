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

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

type checkCustomValueAction struct {
	action.CommonAction
}

// NewCheckCustomValuesAction ["check_custom_values","sourceJsonPath", "customValue",
// "targetJsonPath", "trueFlagReplacement", "falseFlagReplacement"].
func NewCheckCustomValuesAction() action.Action {
	return &checkCustomValueAction{
		CommonAction: action.CommonAction{
			ActionName: "CHECK_CUSTOM_VALUES",
			FixedArgs: []arg.TypeList{
				arg.EventList,
				[]arg.Type{arg.Constant},
				arg.EventList,
				[]arg.Type{arg.Constant},
				[]arg.Type{arg.Constant},
			},
		},
	}
}

func (a *checkCustomValueAction) Init(args []arg.Arg) error {
	a.TargetArg = args[2]
	a.Args = args[:2]
	a.Args = append(a.Args, args[3:]...)
	a.ArgTypes = []common.Type{common.String, common.String, common.String, common.String}
	return nil
}

func (a *checkCustomValueAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	str, _ := args[0].(string)
	customValue, _ := args[1].(string)
	trueFlagReplacement, _ := args[2].(string)
	falseFlagReplacement, _ := args[3].(string)
	if strings.Contains(str, customValue) {
		return a.TargetArg.SetValue(ceCtx, trueFlagReplacement)
	}
	return a.TargetArg.SetValue(ceCtx, falseFlagReplacement)
}
