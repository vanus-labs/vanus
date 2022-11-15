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
	"strconv"
	"strings"
)

func Cast(val interface{}, target Type) (interface{}, error) {
	if target.IsSameType(val) {
		return val, nil
	}
	switch target {
	case String:
		switch val.(type) {
		case int32: // ce attribute
			return strconv.Itoa(int(val.(int32))), nil
		case float64: // ce data json marshal
			return strconv.FormatFloat(val.(float64), 'f', -1, 64), nil
		case bool:
			return strconv.FormatBool(val.(bool)), nil
		}
		// Casting to string is always defined
		return fmt.Sprintf("%v", val), nil
	case Number:
		switch val.(type) {
		case string:
			v, err := strconv.ParseFloat(val.(string), 64)
			if err != nil {
				err = fmt.Errorf("cannot cast from String to Float: %w", err)
			}
			return v, err
		case int32:
			return float64(val.(int32)), nil
		case int64:
			return float64(val.(int64)), nil
		}
		return 0, fmt.Errorf("undefined cast from %v to %v", TypeFromVal(val), target)
	case Bool:
		switch val.(type) {
		case string:
			lowerCase := strings.ToLower(val.(string))
			if lowerCase == "true" {
				return true, nil
			} else if lowerCase == "false" {
				return false, nil
			}
			return false, fmt.Errorf("cannot cast String to Bool, actual value: %v", val)
		}
		return false, fmt.Errorf("undefined cast from %v to %v", TypeFromVal(val), target)
	}

	// AnyType doesn't need casting
	return val, nil
}
