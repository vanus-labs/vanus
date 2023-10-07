// Copyright 2023 Linkall Inc.
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

package parse

import (
	// standard libraries.
	"io"

	// this project.
	"github.com/vanus-labs/vanus/lib/bytes"
)

const basicPlan = "" + //nolint:unused // reserved for future use.
	`................................` + // 0x00
	`...............s................` + // 0x20, slash(0x2f)
	`............................s...` + // 0x40, backslash(0x5c)
	"..\b...\f.......\n...\r.\tu.........." + // 0x60
	`................................` + // 0x80
	`................................` + // 0xa0
	`................................` + // 0xc0
	`................................` //  0xe0

const doubleQuotePlan = "" +
	`................................` + // 0x00
	`..s............s................` + // 0x20, double quote(0x22), slash(0x2f)
	`............................s...` + // 0x40, backslash(0x5c)
	"..\b...\f.......\n...\r.\tu.........." + // 0x60
	`................................` + // 0x80
	`................................` + // 0xa0
	`................................` + // 0xc0
	`................................` //  0xe0

const singleQuotePlan = "" +
	`................................` + // 0x00
	`.......s.......s................` + // 0x20, single quote(0x27), slash(0x2f)
	`............................s...` + // 0x40, backslash(0x5c)
	`................................` + // 0x40
	"..\b...\f.......\n...\r.\tu.........." + // 0x60
	`................................` + // 0x80
	`................................` + // 0xa0
	`................................` + // 0xc0
	`................................` //  0xe0

func ConsumeEscapedWithDoubleQuote(r io.ByteReader, w io.ByteWriter) error {
	return bytes.ConsumeEscaped(r, w, doubleQuotePlan)
}

func ConsumeEscapedWithSingleQuote(r io.ByteReader, w io.ByteWriter) error {
	return bytes.ConsumeEscaped(r, w, singleQuotePlan)
}
