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

package action

import (
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
)

// ["math_add", "toKey","key1","key2"]
func newMathAddActionAction() Action {
	return &commonAction{
		fixedArgs:   []arg.TypeList{arg.EventList, arg.All, arg.All},
		variadicArg: arg.All,
		fn:          function.MathAddFunction,
	}
}

// ["math_sub", "toKey","key1","key2"]
func newMathSubActionAction() Action {
	return &commonAction{
		fixedArgs: []arg.TypeList{arg.EventList, arg.All, arg.All},
		fn:        function.MathSubFunction,
	}
}

// ["math_mul", "toKey","key1","key2"]
func newMathMulActionAction() Action {
	return &commonAction{
		fixedArgs:   []arg.TypeList{arg.EventList, arg.All, arg.All},
		variadicArg: arg.All,
		fn:          function.MathMulFunction,
	}
}

// ["math_div", "toKey","key1","key2"]
func newMathDivActionAction() Action {
	return &commonAction{
		fixedArgs: []arg.TypeList{arg.EventList, arg.All, arg.All},
		fn:        function.MathDivFunction,
	}
}
