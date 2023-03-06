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

	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
)

var LengthFunction = function{
	name:      "length",
	fixedArgs: []common.Type{common.Any},
	fn: func(args []interface{}) (interface{}, error) {
		switch v := args[0].(type) {
		case string:
			return len(v), nil
		case []interface{}:
			return len(v), nil
		case map[string]interface{}:
			return len(v), nil
		default:
			// maybe bool, float64, int32
			return nil, fmt.Errorf("length not support %v", v)
		}
	},
}
