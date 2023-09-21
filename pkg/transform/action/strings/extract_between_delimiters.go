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

	"github.com/vanus-labs/vanus/pkg/transform/action"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/common"
	"github.com/vanus-labs/vanus/pkg/transform/context"
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

	if startDelimiter == "" || endDelimiter == "" {
		return fmt.Errorf("start or end delimiter is empty")
	}
	startIndex := strings.Index(sourceJSONPath, startDelimiter)
	if startIndex < 0 {
		return fmt.Errorf("start delemiter is not exist")
	}
	startIndex += len(startDelimiter)
	endIndex := strings.Index(sourceJSONPath[startIndex:], endDelimiter)
	if endIndex < 0 {
		return fmt.Errorf("end delemiter is not exist")
	}
	endIndex += startIndex
	newValue := sourceJSONPath[startIndex:endIndex]
	return a.TargetArg.SetValue(ceCtx, newValue)
}
