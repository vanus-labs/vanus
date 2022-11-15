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

import "strings"

var JoinFunction = function{
	name:         "JOIN",
	fixedArgs:    []Type{String, String, String},
	variadicArgs: TypePtr(String),
	fn: func(args []interface{}) (interface{}, error) {
		separator := args[0].(string)
		var sb strings.Builder
		for i := 1; i < len(args)-1; i++ {
			sb.WriteString(args[i].(string))
			sb.WriteString(separator)
		}
		sb.WriteString(args[len(args)-1].(string))
		return sb.String(), nil
	},
}

var UpperFunction = function{
	name:      "UPPER_CASE",
	fixedArgs: []Type{String},
	fn: func(args []interface{}) (interface{}, error) {
		return strings.ToUpper(args[0].(string)), nil
	},
}

var LowerFunction = function{
	name:      "LOWER_CASE",
	fixedArgs: []Type{String},
	fn: func(args []interface{}) (interface{}, error) {
		return strings.ToLower(args[0].(string)), nil
	},
}

var AddPrefixFunction = function{
	name:      "ADD_PREFIX",
	fixedArgs: []Type{String, String},
	fn: func(args []interface{}) (interface{}, error) {
		return args[1].(string) + args[0].(string), nil
	},
}

var AddSuffixFunction = function{
	name:      "ADD_SUFFIX",
	fixedArgs: []Type{String, String},
	fn: func(args []interface{}) (interface{}, error) {
		return args[0].(string) + args[1].(string), nil
	},
}

var SplitWithSepFunction = function{
	name:         "SPLIT_WITH_SEP",
	fixedArgs:    []Type{String, String},
	variadicArgs: TypePtr(Number),
	fn: func(args []interface{}) (interface{}, error) {
		s := args[0].(string)
		sep := args[1].(string)
		if len(args) == 2 {
			return strings.Split(s, sep), nil
		}
		return strings.SplitN(s, sep, int(args[2].(float64))), nil
	},
}
