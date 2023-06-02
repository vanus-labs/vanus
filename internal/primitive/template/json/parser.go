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

package json

import (
	// standard libraries.
	"errors"
	"io"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
	"github.com/vanus-labs/vanus/internal/primitive/json/parse"
	"github.com/vanus-labs/vanus/internal/primitive/template"
)

var errParseJSONTemplate = errors.New("cannot parse JSON template")

type parserState int

const (
	waitValue parserState = iota
	waitObjectFirstKey
	waitObjectKey
	waitObjectColon
	waitObjectValue
	waitObjectComma
	waitArrayFirstElement
	waitArrayElement
	waitArrayComma
	waitEOF
)

type templateParser struct {
	stack parserStack
	state parserState
}

func (p *templateParser) parse(text string) (templateNode, error) {
	s := bytes.NewMarkScanner(bytes.UnsafeFromString(text))
	if err := p.doParse(s); err != nil {
		return nil, err
	}
	n, _ := p.stack.pop()
	return n, nil
}

func (p *templateParser) doParse(s *bytes.MarkScanner) error {
	for {
		c, err := skipWhitespace(s)
		if err != nil {
			if err == io.EOF && p.state == waitEOF { //nolint:errorlint // io.EOF is not an error
				return nil
			}
			return err
		}

		switch p.state {
		case waitArrayFirstElement:
			if c == ']' { // empty array
				p.reduce()
				break
			}
			fallthrough
		case waitValue, waitObjectValue, waitArrayElement:
			if err = p.expectValue(c, s); err != nil {
				return err
			}
		case waitObjectFirstKey:
			if c == '}' { // empty object
				p.reduce()
				break
			}
			fallthrough
		case waitObjectKey:
			if c != '"' {
				return errParseJSONTemplate
			}
			sn, err := p.expectString(s)
			if err != nil {
				return err
			}
			mn := &memberNode{key: sn}
			p.stack.push(mn, p.state)
			p.state = waitObjectColon
		case waitObjectColon:
			if c != ':' {
				return errParseJSONTemplate
			}
			p.state = waitObjectValue
		case waitObjectComma:
			switch c {
			case ',':
				p.state = waitObjectKey
			case '}':
				p.reduce()
			default:
				return errParseJSONTemplate
			}
		case waitArrayComma:
			switch c {
			case ',':
				p.state = waitArrayElement
			case ']':
				p.reduce()
			default:
				return errParseJSONTemplate
			}
		default:
			return errParseJSONTemplate
		}
	}
}

func (p *templateParser) expectValue(c byte, s *bytes.MarkScanner) error {
	var n templateNode
	switch c {
	case '{':
		p.stack.push(&objectNode{}, p.state)
		p.state = waitObjectFirstKey
		return nil
	case '[':
		p.stack.push(&arrayNode{}, p.state)
		p.state = waitArrayFirstElement
		return nil
	case '<':
		nn, err := p.expectVariable(s)
		if err != nil {
			return err
		}
		n = nn
	case '"':
		dsn, err := p.expectDynamicString(s)
		if err != nil {
			return err
		}
		n = dsn
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		nn, err := p.expectNumber(c, s)
		if err != nil {
			return err
		}
		n = nn
	case 't':
		if err := exceptTrueExt(s); err != nil {
			return err
		}
		n = True
	case 'f':
		if err := exceptFalseExt(s); err != nil {
			return err
		}
		n = False
	case 'n':
		if err := exceptNullExt(s); err != nil {
			return err
		}
		n = Null
	default:
		return errParseJSONTemplate
	}

	p.reduceExt(n)
	return nil
}

func (p *templateParser) expectVariable(s *bytes.MarkScanner) (templateNode, error) {
	original, path, err := template.ExpectVariable(s)
	if err != nil {
		return nil, err
	}
	if path != nil {
		return &jsonPathNode{path: path, original: original}, nil
	}
	return &variableNode{name: string(original)}, nil
}

func (p *templateParser) expectDynamicString(s *bytes.MarkScanner) (templateNode, error) {
	var elements []templateNode
	for {
		// string
		m := s.Mark(0)
		next, err := consumeDynamicString(s, bytes.DummyWriter)
		if err != nil {
			return nil, err
		}
		if bs := s.Since(m, -1); len(bs) != 0 {
			elements = append(elements, &stringNode{val: unescapeBracket(bs)})
		}

		if !next {
			break
		}

		// variable
		nn, err := p.expectVariable(s)
		if err != nil {
			return nil, errInvalidDynamicString
		}
		elements = append(elements, nn)
	}

	switch len(elements) {
	case 0:
		return &stringNode{val: []byte{}}, nil
	case 1:
		switch n := elements[0].(type) {
		case *stringNode:
			return n, nil
		default:
			return &dynamicStringNode{elements}, nil
		}
	default:
		return &dynamicStringNode{elements}, nil
	}
}

func (p *templateParser) expectString(s *bytes.MarkScanner) (*stringNode, error) {
	m := s.Mark(0) // exclude '"'

	if err := parse.ConsumeDoubleQuotedString(s, bytes.DummyWriter); err != nil {
		return nil, errInvalidString
	}

	return &stringNode{val: unescapeBracket(s.Since(m, -1))}, nil
}

func (p *templateParser) expectNumber(c byte, s *bytes.MarkScanner) (*numberNode, error) {
	m := s.Mark(-1) // include c

	_, err := expectNumberExt(c, s)
	if err != nil {
		return nil, err
	}

	return &numberNode{val: s.Since(m, 0)}, nil
}

func (p *templateParser) reduce() {
	n, s := p.stack.pop()

	// resume last state
	p.state = s

	// then do reduce
	p.reduceExt(n)
}

func (p *templateParser) reduceExt(n templateNode) {
	switch p.state {
	case waitObjectValue:
		p.reduceObjectMember(n)
	case waitArrayElement, waitArrayFirstElement:
		p.reduceArrayElement(n)
	case waitValue:
		p.stack.push(n, p.state)
		p.state = waitEOF
	}
}

func (p *templateParser) reduceObjectMember(n templateNode) {
	nn, _ := p.stack.pop()
	mn, _ := nn.(*memberNode)
	mn.value = n

	nn, _ = p.stack.peek()
	on, _ := nn.(*objectNode)
	on.members = append(on.members, mn)

	// expect next member
	p.state = waitObjectComma
}

func (p *templateParser) reduceArrayElement(n templateNode) {
	nn, _ := p.stack.peek()
	an, _ := nn.(*arrayNode)
	an.elements = append(an.elements, n)

	// expect next element
	p.state = waitArrayComma
}
