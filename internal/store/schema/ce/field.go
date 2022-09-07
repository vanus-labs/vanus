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

package ce

import "github.com/linkall-labs/vanus/internal/store/block"

const (
	CloudEvent uint16 = 0x6563 // ASCII of "ce" in little endian
	End        uint16 = 0x6465 // ASCII of "ed" in little endian
	Index      uint16 = 0x7864 // ASCII of "dx" in little endian
)

func EntryType(entry block.Entry) uint16 {
	return entry.GetUint16(EntryTypeOrdinal)
}

func SequenceNumber(entry block.Entry) int64 {
	return entry.GetInt64(SequenceNumberOrdinal)
}

// Stime returns the value of stime field, which is a millisecond timestamp when the Entry will be written to Block.
func Stime(entry block.Entry) int64 {
	return entry.GetInt64(StimeOrdinal)
}
