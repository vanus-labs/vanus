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
	"time"

	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function/util"
)

var DateFormatFunction = function{
	name:         "DATE_FORMAT",
	fixedArgs:    []common.Type{common.String, common.String},
	variadicArgs: common.TypePtr(common.String),
	fn: func(args []interface{}) (interface{}, error) {
		t, err := time.ParseInLocation(time.RFC3339, args[0].(string), time.UTC)
		if err != nil {
			return nil, err
		}
		loc := time.UTC
		if len(args) > 2 && args[2].(string) != "" {
			loc, err = time.LoadLocation(args[2].(string))
			if err != nil {
				return nil, err
			}
		}
		format := util.ConvertFormat2Go(args[1].(string))
		return t.In(loc).Format(format), nil
	},
}

var UnixTimeFormatFunction = function{
	name:         "UNIX_TIME_FORMAT",
	fixedArgs:    []common.Type{common.Number, common.String},
	variadicArgs: common.TypePtr(common.String),
	fn: func(args []interface{}) (interface{}, error) {
		t := time.Unix(int64(args[0].(float64)), 0)
		loc := time.UTC
		if len(args) > 2 && args[2].(string) != "" {
			var err error
			loc, err = time.LoadLocation(args[2].(string))
			if err != nil {
				return nil, err
			}
		}
		format := util.ConvertFormat2Go(args[1].(string))
		return t.In(loc).Format(format), nil
	},
}
