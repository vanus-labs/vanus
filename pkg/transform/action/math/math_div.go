// Copyright 2022 Linkall Inc.
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

package math

import (
	"github.com/vanus-labs/vanus/pkg/transform/action"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/function"
)

// NewMathDivAction ["math_div", "toKey","key1","key2"].
func NewMathDivAction() action.Action {
	a := &action.FunctionAction{}
	a.CommonAction = action.CommonAction{
		ActionName: "MATH_DIV",
		FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All},
		Fn:         function.MathDivFunction,
	}
	return a
}
