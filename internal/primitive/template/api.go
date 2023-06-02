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

package template

import (
	// standard libraries.
	"unicode"

	// third-party libraries.
	"golang.org/x/text/unicode/rangetable"
)

var IdentifierRangeTable = rangetable.Merge(unicode.Letter, unicode.Nd, rangetable.New('_', '-'))

type Template interface {
	ContentType() string
	Execute(model any, variables map[string]any) ([]byte, error)
}
