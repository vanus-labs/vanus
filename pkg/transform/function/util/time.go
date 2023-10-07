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

package util

import (
	"bytes"
	"time"
)

var formats = map[byte]string{
	'd': "02",   // Day:    Day of the month, 2 digits with leading zeros. Eg: 01 to 31.
	'm': "01",   // Month:  Numeric representation of a month, with leading zeros. Eg: 01 through 12.
	'Y': "2006", // Year:   A full numeric representation of a year, 4 digits. Eg: 1999 or 2003.
	'y': "06",   // Year:   A two digit representation of a year. Eg: 99 or 03.
	'h': "03",   // Time:   12-hour format of an hour with leading zeros. Eg: 01 through 12.
	'H': "15",   // Time:   24-hour format of an hour with leading zeros. Eg: 00 through 23.
	'i': "04",   // Time:   Minutes with leading zeros. Eg: 00 to 59.
	's': "05",   // Time:   Seconds with leading zeros. Eg: 00 through 59.
}

// ConvertFormat2Go converts format to layout.
func ConvertFormat2Go(format string) string {
	buffer := bytes.NewBuffer(nil)
	for i := 0; i < len(format); i++ {
		if layout, ok := formats[format[i]]; ok {
			buffer.WriteString(layout)
		} else {
			switch format[i] {
			case '\\': // raw output, no parse
				buffer.WriteByte(format[i+1])
				i++
				continue
			default:
				buffer.WriteByte(format[i])
			}
		}
	}
	return buffer.String()
}

func TimezoneFromString(timezone string) *time.Location {
	if timezone == "" {
		return time.UTC
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.UTC
	}
	return location
}
