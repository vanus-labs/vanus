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
	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
)

type extractPattern struct {
	action.CommonAction
}

// NewExtractMissingAction ["extract_missing", "sourceJSONPath", "targetJSONPath",
// "trueFlagReplacement", "falseFlagReplacement"].
func NewExtractPatternAction() action.Action {
	return &extractPattern{
		CommonAction: action.CommonAction{
			ActionName: "EXTRACT_PATTERN",
			FixedArgs: []arg.TypeList{
				arg.EventList,
				arg.EventList,
				[]arg.Type{arg.Constant},
				[]arg.Type{arg.Constant},
			},
		},
	}
}

func (a *extractPattern) Init(args []arg.Arg) error {
	a.TargetArg = args[1]
	a.Args = []arg.Arg{args[0]}
	a.Args = append(a.Args, args[2:]...)
	a.ArgTypes = []common.Type{common.String, common.Any, common.Any}
	return nil
}

func (a *extractPattern	) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	sourceJSONPath, _ := args[0].(string)
	trueFlagReplacement := args[1]
	falseFlagReplacement := args[2]
	
	if sourceJSONPath == "a*" {
		ceCtx.Set(a.TargetArg, trueFlagReplacement)
	} else {
		ceCtx.Set(a.TargetArg, falseFlagReplacement)
	}
	 return ceCtx.Set(a.TargetArg, TargetJSONPath)
}




