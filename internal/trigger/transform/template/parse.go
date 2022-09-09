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

	"github.com/linkall-labs/vanus/pkg/util"
)

type OutputType int

const (
	TEXT = iota
	JSON
)

type Parser struct {
	leftDelim  string
	rightDelim string
	nodes      []Node
	OutputType OutputType
}

func NewParser() *Parser {
	p := &Parser{}
	p.init()
	return p
}

func (p *Parser) init() {
	p.leftDelim = "${"
	p.rightDelim = "}"
}

func (p *Parser) GetNodes() []Node {
	return p.nodes
}

func (p *Parser) addNode(node Node) {
	p.nodes = append(p.nodes, node)
}

func (p *Parser) parseType(text string) {
	for pos := 0; pos < len(text); pos++ {
		c := text[pos]
		if util.IsSpace(c) {
			continue
		}
		if c == '{' {
			p.OutputType = JSON
		} else {
			p.OutputType = TEXT
		}
		break
	}
}

// isJSONKeyColon check colon is key end colon,maybe:
// "key": ${v}
// "key": ":${v}"
// "key": "other:${v}"
// "key": "\":${v}" .
func isJSONKeyColon(text string, pos int) bool {
	var hasQuota bool
	for i := pos; i >= 0; i-- {
		c := text[i]
		if util.IsSpace(c) {
			continue
		}
		switch c {
		case '"':
			if hasQuota {
				return false
			}
			hasQuota = true
		case '\\', ':':
			return false
		default:
			return hasQuota
		}
	}
	return false
}
func isStringVar(text string, pos int) bool {
	for i := pos; i >= 0; i-- {
		c := text[i]
		if util.IsSpace(c) {
			continue
		}
		switch c {
		case '"':
			return true
		case ':':
			// 是否是json key后面的冒号
			b := isJSONKeyColon(text, i-1)
			return !b
		}
	}
	return false
}

func (p *Parser) Parse(text string) {
	p.parseType(text)
	var pos int
	leftDelimLen := len(p.leftDelim)
	rightDelimLen := len(p.rightDelim)
	for {
		x := strings.Index(text[pos:], p.leftDelim)
		if x < 0 {
			p.addNode(p.newConstant(text[pos:]))
			break
		}
		ldp := pos + x + leftDelimLen
		y := strings.Index(text[ldp:], p.rightDelim)
		if y < 0 {
			continue
		}
		var stringVar bool
		if p.OutputType == JSON {
			stringVar = isStringVar(text, pos+x-1)
		}
		if x > 0 {
			p.addNode(p.newConstant(text[pos : pos+x]))
		}
		if stringVar {
			p.addNode(p.newStringVariable(text[ldp : ldp+y]))
		} else {
			p.addNode(p.newVariable(text[ldp : ldp+y]))
		}
		pos = ldp + y + rightDelimLen
		if pos == len(text) {
			break
		}
	}
}
