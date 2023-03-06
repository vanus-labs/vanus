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
	"strings"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

type ExtractBetweenDelimitersAction struct {
	action.CommonAction
}

// NewExtractBetweenDelimitersAction [sourceJSONPath, targetJSONPath, startDelimiter, endDelimiter].
func NewExtractBetweenDelimitersAction() action.Action {
	return &ExtractBetweenDelimitersAction{
		CommonAction: action.CommonAction{
			ActionName: "EXTRACT_BETWEEN_DELIMITERS",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.EventList, arg.All, arg.All},
		},
	}
}

func (a *ExtractBetweenDelimitersAction) Init(args []arg.Arg) error {
	a.TargetArg = args[1]
	a.Args = args[:1]
	a.Args = append(a.Args, args[2:]...)
	a.ArgTypes = []common.Type{common.String, common.String, common.String}
	return nil
}

func (a *ExtractBetweenDelimitersAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	sourceJSONPath, _ := args[0].(string)
	startDelimiter, _ := args[1].(string)
	endDelimiter, _ := args[2].(string)

	if strings.Contains(sourceJSONPath, startDelimiter) && strings.Contains(sourceJSONPath, endDelimiter) {
		firstSplit := strings.Split(sourceJSONPath, startDelimiter)
		secondSplit := strings.Split(firstSplit[1], endDelimiter)
		newValue := secondSplit[0]
		return a.TargetArg.SetValue(ceCtx, newValue)
	}
	return fmt.Errorf("the start and/or end pattern is not present in the input string")
}
