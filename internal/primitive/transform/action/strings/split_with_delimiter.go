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
	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
)

type splitWithDelimiterAction struct {
	action.FunctionAction
}

// NewSplitWithDelimiterAction ["split_with_delimiter","sourceJsonPath", "delimiter", "targetJsonPath"].
func NewSplitWithDelimiterAction() action.Action {
	a := &splitWithDelimiterAction{}
	a.CommonAction = action.CommonAction{
		ActionName: "SPLIT_WITH_DELIMITER",
		FixedArgs:  []arg.TypeList{arg.EventList, []arg.Type{arg.Constant}, []arg.Type{arg.EventData}},
		Fn:         function.SplitWithSepFunction,
	}
	return a
}

func (a *splitWithDelimiterAction) Init(args []arg.Arg) error {
	a.TargetArg = args[2]
	a.Args = args[:2]
	a.ArgTypes = []common.Type{common.String, common.String}
	return nil
}
