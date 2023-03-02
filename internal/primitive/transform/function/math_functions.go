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

package function

import (
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
)

var MathAddFunction = function{
	name:         "MATH_ADD",
	fixedArgs:    []common.Type{common.Float, common.Float},
	variadicArgs: common.TypePtr(common.Float),
	fn: func(args []interface{}) (interface{}, error) {
		var sum float64
		for i := 0; i < len(args); i++ {
			v, _ := args[i].(float64)
			sum += v
		}
		return sum, nil
	},
}

var MathSubFunction = function{
	name:      "MATH_SUB",
	fixedArgs: []common.Type{common.Float, common.Float},
	fn: func(args []interface{}) (interface{}, error) {
		return args[0].(float64) - args[1].(float64), nil
	},
}

var MathMulFunction = function{
	name:         "MATH_MUL",
	fixedArgs:    []common.Type{common.Float, common.Float},
	variadicArgs: common.TypePtr(common.Float),
	fn: func(args []interface{}) (interface{}, error) {
		sum := float64(1)
		for i := 0; i < len(args); i++ {
			v, _ := args[i].(float64)
			sum *= v
		}
		return sum, nil
	},
}

var MathDivFunction = function{
	name:      "MATH_DIV",
	fixedArgs: []common.Type{common.Float, common.Float},
	fn: func(args []interface{}) (interface{}, error) {
		if args[1].(float64) == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return args[0].(float64) / args[1].(float64), nil
	},
}
