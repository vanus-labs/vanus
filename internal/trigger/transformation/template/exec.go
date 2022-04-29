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

package template

import (
	"strings"
)

func (p *Parser) Execute(data map[string][]byte) string {
	var sb strings.Builder
	for _, node := range p.GetNodes() {
		switch node.Type() {
		case Constant:
			sb.WriteString(node.Value())
		case Variable:
			v, exist := data[node.Value()]
			if !exist || len(v) == 0 {
				if p.OutputType == JSON {
					sb.WriteString("null")
				}
			} else {
				sb.Write(v)
			}
		case StringVariable:
			v, exist := data[node.Value()]
			if !exist || len(v) == 0 {
				continue
			}
			sb.Write(v)
		}
	}
	return sb.String()
}
