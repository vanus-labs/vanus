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

var mergeFunction = function{
	name:           "MERGE",
	fixedArgs:      []Type{StringArray, String, String},
	targetArgIndex: 2,
	fn: func(args []interface{}) (interface{}, error) {
		separator := args[1].(string)
		var sb strings.Builder
		elems := args[0].([]string)
		for i := 0; i < len(elems); i++ {
			if i != 0 {
				sb.WriteString(separator)
			}
			sb.WriteString(elems[i])
		}
		return sb.String(), nil
	},
}

var replaceValueFunction = function{
	name:      "REPLACE_VALUE",
	fixedArgs: []Type{String, String, String},
	fn: func(args []interface{}) (interface{}, error) {
		return strings.ReplaceAll(args[0].(string), args[0].(string), args[0].(string)), nil
	},
}
