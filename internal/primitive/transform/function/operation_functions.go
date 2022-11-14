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

var deleteFunction = function{
	name:      "DELETE",
	fixedArgs: []Type{String},
	fn: func(args []interface{}) (interface{}, error) {
		return nil, nil
	},
}

var createFunction = function{
	name:      "CREATE",
	fixedArgs: []Type{String, Any},
	fn: func(args []interface{}) (interface{}, error) {
		return args[1], nil
	},
}

var replaceFunction = function{
	name:           "REPLACE",
	targetArgIndex: 1,
	fixedArgs:      []Type{String, Any},
	fn: func(args []interface{}) (interface{}, error) {
		return args[1], nil
	},
}

var moveFunction = function{
	name:           "MOVE",
	targetArgIndex: 1,
	fixedArgs:      []Type{Any, String},
	fn: func(args []interface{}) (interface{}, error) {
		return args[0], nil
	},
}

var renameFunction = function{
	name:           "RENAME",
	targetArgIndex: 1,
	fixedArgs:      []Type{Any, String},
	fn: func(args []interface{}) (interface{}, error) {
		return args[0], nil
	},
}
