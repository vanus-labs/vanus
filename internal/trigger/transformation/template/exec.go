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

type DataType int

const (
	Null = iota
	Text
	Other
)

type Data struct {
	DataType
	Raw []byte
}

func (d Data) String() string {
	switch d.DataType {
	case Null:
		return "null"
	default:
		return string(d.Raw)
	}
}

func NewNullData() Data {
	return Data{DataType: Null}
}

func NewTextData(d []byte) Data {
	return Data{Text, d}
}

func NewOtherData(d []byte) Data {
	return Data{Other, d}
}

func (p *Parser) executeJSON(data map[string]Data) string {
	var sb strings.Builder
	for _, node := range p.GetNodes() {
		switch node.Type() {
		case Constant:
			sb.WriteString(node.Value())
		case Variable:
			v, exist := data[node.Value()]
			if !exist || v.DataType == Null {
				sb.WriteString("null")
			}
			if v.DataType == Text {
				sb.WriteString("\"")
			}
			sb.Write(v.Raw)
			if v.DataType == Text {
				sb.WriteString("\"")
			}
		case StringVariable:
			v, exist := data[node.Value()]
			if !exist || v.DataType == Null {
				continue
			}
			sb.Write(v.Raw)
		}
	}
	return sb.String()
}

func (p *Parser) executeText(data map[string]Data) string {
	var sb strings.Builder
	for _, node := range p.GetNodes() {
		switch node.Type() {
		case Constant:
			sb.WriteString(node.Value())
		case Variable, StringVariable:
			v, exist := data[node.Value()]
			if !exist || v.DataType == Null {
				continue
			}
			sb.Write(v.Raw)
		}
	}
	return sb.String()
}

func (p *Parser) Execute(data map[string]Data) string {
	switch p.OutputType {
	case JSON:
		return p.executeJSON(data)
	default:
		return p.executeText(data)
	}
}
