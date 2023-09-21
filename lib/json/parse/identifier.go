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

import "unicode/utf8"

func ExceptNameFirst(r rune) bool {
	return r == '_' ||
		r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' ||
		r >= 0x80 && r <= utf8.MaxRune
}

func ExceptNameChar(r rune) bool {
	return r == '_' ||
		r >= '0' && r <= '9' ||
		r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' ||
		r >= 0x80 && r <= utf8.MaxRune
}
