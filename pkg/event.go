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

package pkg

import "fmt"

const maxEventAttrNameLength = 20

func ValidateEventAttrName(attr string) error {
	if len(attr) > maxEventAttrNameLength {
		return fmt.Errorf("CloudEvents attribute names length SHOULD NOT exceed 20 characters")
	}
	for _, c := range attr {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
			return fmt.Errorf("CloudEvents attribute names MUST consist of lower-case letters ('a' to 'z') or digits ('0' to '9') from the ASCII character set")
		}
	}
	return nil
}
