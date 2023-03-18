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

	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
)

type extractBetweenPositionsAction struct {
	action.CommonAction
}

// NewExtractBetweenPositionsAction ["extract_between_positions",
// "sourceJSONPath", "targetJsonPath", "startPosition", "endPosition"]
func NewExtractBetweenPositionsAction() action.Action {
	return &extractBetweenPositionsAction{
		CommonAction: action.CommonAction{
			ActionName: "EXTRACT_BETWEEN_POSITIONS",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.EventList, arg.All, arg.All},
		},
	}
}

func (a *extractBetweenPositionsAction) Init(args []arg.Arg) error {
	a.TargetArg = args[1]
	a.Args = []arg.Arg{args[0]}
	a.Args = append(a.Args, args[2:]...)
	a.ArgTypes = []common.Type{common.String, common.Int, common.Int}
	return nil
}

func (a *extractBetweenPositionsAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}

	sourceJSONPath, _ := args[0].(string)
	startPosition, _ := args[1].(int)
	endPosition, _ := args[2].(int)

	if startPosition > len(sourceJSONPath) {
		return fmt.Errorf("start position must be equal or less than the length of the string")
	}
	if startPosition <= 0 {
		return fmt.Errorf("start position must be more than zero")
	}
	if endPosition > len(sourceJSONPath) {
		return fmt.Errorf("end position must be equal or less than the length of the string")
	}
	if startPosition > endPosition {
		return fmt.Errorf("start position must be be equal or less than end position")
	}
	return a.TargetArg.SetValue(ceCtx, sourceJSONPath[startPosition-1:endPosition])
}
