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
	"encoding/json"
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

type splitWithIntervalsAction struct {
	action.CommonAction
}

// NewSplitWithIntervalsAction["sourceJSONPath", "startPosition", "splitInterval", "targetJsonPath"].
func NewSplitWithIntervalsAction() action.Action {
	return &splitWithIntervalsAction{
		CommonAction: action.CommonAction{
			ActionName: "SPLIT_WITH_INTERVALS",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All, arg.EventList},
		},
	}
}

func (a *splitWithIntervalsAction) Init(args []arg.Arg) error {
	a.TargetArg = args[3]
	a.Args = args[:3]
	a.ArgTypes = []common.Type{common.String, common.Number, common.Number}
	return nil
}

func (a *splitWithIntervalsAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}

	v, _ := a.TargetArg.Evaluate(ceCtx)
	if v != nil {
		return fmt.Errorf("key %s exist", a.TargetArg.Original())
	}

	sourceJSONPath, _ := args[0].(string)
	startPosition := int(args[1].(float64))
	splitInterval := int(args[2].(float64))

	// split string
	var substrings []string
	if startPosition > len(sourceJSONPath) {
		// if startPosition is beyond the end of the string, return an error
		return fmt.Errorf("start position must be less than the length of the string")
	}

	// split the string according to the specified interval
	substrings = []string{sourceJSONPath[:startPosition]}
	for i := startPosition; i < len(sourceJSONPath); i += splitInterval {
		end := i + splitInterval
		if end > len(sourceJSONPath) {
			end = len(sourceJSONPath)
		}
		substrings = append(substrings, sourceJSONPath[i:end])
	}

	j, err := json.Marshal(substrings)
	if err != nil {
		return fmt.Errorf("error: %w", err)
	}

	return a.TargetArg.SetValue(ceCtx, j)
}
