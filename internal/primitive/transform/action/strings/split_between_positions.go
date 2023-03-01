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
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

type splitBetweenPositionsAction struct {
	action.CommonAction
}

// NewSplitBetweenPositionsAction["sourceJSONPath", "startPosition", "endPosition", "targetJsonPath"].
func NewSplitBetweenPositionsAction() action.Action {
	return &splitBetweenPositionsAction{
		CommonAction: action.CommonAction{
			ActionName: "SPLIT_BETWEEN_POSITIONS",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All, []arg.Type{arg.EventData}},
		},
	}
}

func (a *splitBetweenPositionsAction) Init(args []arg.Arg) error {
	a.TargetArg = args[3]
	a.Args = args[:3]
	a.ArgTypes = []common.Type{common.String, common.Int, common.Int}
	return nil
}

func (a *splitBetweenPositionsAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}

	v, _ := a.TargetArg.Evaluate(ceCtx)
	if v != nil {
		return fmt.Errorf("key %s exists", a.TargetArg.Original())
	}

	sourceJSONPath, _ := args[0].(string)
	startPosition, _ := args[1].(int)
	endPosition, _ := args[2].(int)

	var substrings []string

	if startPosition+endPosition > len(sourceJSONPath) {
		// if sum of start position and end position is greater than the length of string, return an error
		return fmt.Errorf("sum of start position and end position must be equal to or less than the length of the string")
	}

	substrings = []string{
		sourceJSONPath[:startPosition],
		sourceJSONPath[startPosition : len(sourceJSONPath)-endPosition],
		sourceJSONPath[len(sourceJSONPath)-endPosition:],
	}

	return a.TargetArg.SetValue(ceCtx, substrings)
}
