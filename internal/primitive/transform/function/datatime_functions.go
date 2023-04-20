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

	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/function/util"
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
	fixedArgs:    []common.Type{common.Int, common.String},
	variadicArgs: common.TypePtr(common.String),
	fn: func(args []interface{}) (interface{}, error) {
		t := time.Unix(int64(args[0].(int)), 0)
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

var ConvertTimeZoneFunction = function{
	name:         "CONVERT_TIMEZONE",
	fixedArgs:    []common.Type{common.String, common.String, common.String},
	variadicArgs: common.TypePtr(common.String),
	fn: func(args []interface{}) (interface{}, error) {
		var dateTimeFormat string
		if len(args) > 4 && args[4].(string) == "" {
			dateTimeFormat = "2006-01-02 15:04:05"
		} else {
			dateTimeFormat = args[4].(string)
		}
		// Check if the source time value is a string.
		sourceTimeString, ok := args[0].(string)
		if !ok {
			return nil, nil
		}

		sourceTimeParsed, err := time.ParseInLocation(
			dateTimeFormat, 
			sourceTimeString, 
			util.TimezoneFromString(args[1].(string)),
		)
		if err != nil {
			return nil, err
		}

		// Convert the target time to the destination timezone.
		targetTimeParsed := sourceTimeParsed.In(util.TimezoneFromString(args[2].(string)))

		// Format the target time value.
		targetTimeString := targetTimeParsed.Format(dateTimeFormat)

		return targetTimeString, nil
	},
}
