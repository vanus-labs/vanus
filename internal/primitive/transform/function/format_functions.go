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

import "time"

var formatDateFunction = function{
	name:             "FORMAT_DATE",
	fixedArgs:        []Type{String, String, String},
	sourceTargetSame: true,
	fn: func(args []interface{}) (interface{}, error) {
		t, err := time.Parse(args[1].(string), args[0].(string))
		if err != nil {
			return nil, err
		}
		return t.Format(args[2].(string)), nil
	},
}

var formatUnixTimeFunction = function{
	name:             "FORMAT_UNIX_TIME",
	fixedArgs:        []Type{Number, String},
	sourceTargetSame: true,
	fn: func(args []interface{}) (interface{}, error) {
		t := time.UnixMilli(int64(args[0].(float64)))
		return t.Format(args[1].(string)), nil
	},
}
