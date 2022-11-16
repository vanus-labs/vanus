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
	"strings"
	"time"
)

// convertTimeFormat2Go convert "yyyy-mm-dd HH:MM:SS" to "2006-01-02 15:04:05"
func convertTimeFormat2Go(format string) string {
	v := strings.Replace(format, "yyyy", "2006", -1)
	v = strings.Replace(v, "mm", "01", -1)
	v = strings.Replace(v, "dd", "02", -1)
	v = strings.Replace(v, "HH", "15", -1)
	v = strings.Replace(v, "MM", "04", -1)
	v = strings.Replace(v, "SS", "05", -1)
	return v
}

var FormatDateFunction = function{
	name:      "FORMAT_DATE",
	fixedArgs: []Type{String, String, String},
	fn: func(args []interface{}) (interface{}, error) {
		fromFormat := convertTimeFormat2Go(args[1].(string))
		t, err := time.Parse(fromFormat, args[0].(string))
		if err != nil {
			return nil, err
		}
		toFormat := convertTimeFormat2Go(args[2].(string))
		return t.Format(toFormat), nil
	},
}

var FormatUnixTimeFunction = function{
	name:      "FORMAT_UNIX_TIME",
	fixedArgs: []Type{Number, String},
	fn: func(args []interface{}) (interface{}, error) {
		t := time.Unix(int64(args[0].(float64)), 0)
		toFormat := convertTimeFormat2Go(args[1].(string))
		return t.UTC().Format(toFormat), nil
	},
}
