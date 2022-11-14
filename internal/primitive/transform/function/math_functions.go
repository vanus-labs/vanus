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

import "fmt"

var mathAddFunction = function{
	name:         "MATH_ADD",
	fixedArgs:    []Type{String, Number, Number},
	variadicArgs: TypePtr(Number),
	fn: func(args []interface{}) (interface{}, error) {
		var sum float64
		for i := 1; i < len(args); i++ {
			sum += args[i].(float64)
		}
		return sum, nil
	},
}

var mathSubFunction = function{
	name:      "MATH_SUB",
	fixedArgs: []Type{String, Number, Number},
	fn: func(args []interface{}) (interface{}, error) {
		return args[1].(float64) - args[2].(float64), nil
	},
}

var mathMulFunction = function{
	name:         "MATH_MUL",
	fixedArgs:    []Type{String, Number, Number},
	variadicArgs: TypePtr(Number),
	fn: func(args []interface{}) (interface{}, error) {
		sum := float64(1)
		for i := 1; i < len(args); i++ {
			sum *= args[i].(float64)
		}
		return sum, nil
	},
}

var mathDivFunction = function{
	name:      "MATH_DIV",
	fixedArgs: []Type{String, Number, Number},
	fn: func(args []interface{}) (interface{}, error) {
		if args[2].(float64) == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return args[1].(float64) / args[2].(float64), nil
	},
}
