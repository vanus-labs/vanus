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

const (
	EventArgPrefix     = "$."
	EventDataArgPrefix = EventArgPrefix + "data"
)

type Template struct {
	parser      *parser
	exist       bool
	contentType string
}

func NewTemplate() *Template {
	return &Template{
		parser: newParser(),
	}
}

func (t *Template) Exist() bool {
	return t.exist
}

func (t *Template) ContentType() string {
	return t.contentType
}

func (t *Template) Parse(text string) {
	if text == "" {
		t.exist = false
		return
	}
	t.exist = true
	t.parser.parse(text)
}

type parser struct {
	leftDelim  string
	rightDelim string
	nodes      []Node
}

func newParser() *parser {
	p := &parser{}
	p.init()
	return p
}

func (p *parser) init() {
	p.leftDelim = "<"
	p.rightDelim = ">"
}

func (p *parser) getNodes() []Node {
	return p.nodes
}

func (p *parser) addNode(node Node) {
	p.nodes = append(p.nodes, node)
}

// isJSONKeyColon check colon is key end colon,maybe:
// "key": <v>
// "key": ":<v>"
// "key": "other:<v>"
// "key": "\":<v>" .
func isJSONKeyColon(text string, pos int) bool {
	var hasQuote bool
	for i := pos; i >= 0; i-- {
		c := text[i]
		if util.IsSpace(c) {
			continue
		}
		switch c {
		case '"':
			if hasQuote {
				return false
			}
			hasQuote = true
		case '\\', ':':
			return false
		default:
			return hasQuote
		}
	}
	return false
}

func (p *parser) isStringValue(text string, pos int) bool {
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
			if b {
				return false
			}
			return true
		}
	}
	return false
}

func parseNode(name string, isValueStr bool) Node {
	l := len(name)
	eventArgLen := len(EventArgPrefix)
	if l >= eventArgLen && name[:eventArgLen] == EventArgPrefix {
		eventArgLen = len(EventDataArgPrefix)
		if name == EventDataArgPrefix || (l > eventArgLen && name[:eventArgLen+1] == EventDataArgPrefix+".") {
			return newEventDataNode(name, isValueStr)
		}
		return newEventAttributeNode(name, isValueStr)
	}
	return newDefine(name, isValueStr)
}

func (p *parser) parse(text string) {
	var pos int
	leftDelimLen := len(p.leftDelim)
	rightDelimLen := len(p.rightDelim)
	for {
		x := strings.Index(text[pos:], p.leftDelim)
		if x < 0 {
			p.addNode(p.newConstant(text[pos:]))
			break
		}
		// todo escape
		ldp := pos + x + leftDelimLen
		y := strings.Index(text[ldp:], p.rightDelim)
		if y < 0 {
			p.addNode(p.newConstant(text[pos:]))
			break
		}
		if x > 0 {
			p.addNode(p.newConstant(text[pos : pos+x]))
		}
		strValue := p.isStringValue(text, pos+x-1)
		p.addNode(parseNode(text[ldp:ldp+y], strValue))
		pos = ldp + y + rightDelimLen
		if pos == len(text) {
			break
		}
	}
}
