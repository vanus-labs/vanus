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
	"bytes"

	// third-party libraries.
	"github.com/ohler55/ojg/jp"
)

type templateGenerator struct {
	stack    generatorStack
	buf      bytes.Buffer
	segments []templateSegment
}

func (g *templateGenerator) generate(root templateNode) []templateSegment {
	g.stack.push(root)

	for {
		n, i := g.stack.peek()

		switch node := n.(type) {
		case *objectNode:
			if g.generateObjectNode(node, i) {
				continue
			}
		case *arrayNode:
			if g.generateArrayNode(node, i) {
				continue
			}
		case *dynamicStringNode:
			g.generateDynamicStringNode(node)
		case *variableNode:
			g.insertSegment(&variableSegment{name: node.name})
		case *stringNode:
			g.buf.WriteByte('"')
			g.buf.Write(node.val)
			g.buf.WriteByte('"')
		case *numberNode:
			g.buf.Write(node.val)
		case *boolNode:
			if node.val {
				g.buf.Write([]byte("true"))
			} else {
				g.buf.Write([]byte("false"))
			}
		case *nullNode:
			g.buf.Write([]byte("null"))
		default:
			panic("unexpected node type") // unreachable
		}

		if g.stack.pop() {
			break
		}
	}

	g.packLiteral()

	return g.segments
}

func (g *templateGenerator) generateObjectNode(node *objectNode, i int) bool {
	if i >= len(node.members) {
		g.buf.WriteByte('}')
		return false
	}

	if i == 0 {
		g.buf.WriteByte('{')
	}

	mn := node.members[i]
	switch n := mn.value.(type) {
	case *jsonPathNode:
		key := make([]byte, len(mn.key.val)+3)
		key[0] = '"'
		copy(key[1:], mn.key.val)
		key[len(key)-2] = '"'
		key[len(key)-1] = ':'
		g.insertSegment(&memberSegment{
			key:   key,
			value: jp.MustParse(n.original),
		})
		g.stack.advance()
	default:
		if i != 0 {
			g.buf.WriteByte(',')
		}
		g.buf.WriteByte('"')
		g.buf.Write(mn.key.val)
		g.buf.Write([]byte(`":`))
		g.stack.advanceThenPush(n)
	}
	return true
}

func (g *templateGenerator) generateArrayNode(node *arrayNode, i int) bool {
	if i >= len(node.elements) {
		g.buf.WriteByte(']')
		return false
	}

	if i == 0 {
		g.buf.WriteByte('[')
	}

	en := node.elements[i]
	switch n := en.(type) {
	case *jsonPathNode:
		g.insertSegment(&elementSegment{
			value: jp.MustParse(n.original),
		})
		g.stack.advance()
	default:
		if i != 0 {
			g.buf.WriteByte(',')
		}
		g.stack.advanceThenPush(n)
	}
	return true
}

func (g *templateGenerator) generateDynamicStringNode(node *dynamicStringNode) {
	g.buf.WriteByte('"')
	for _, en := range node.elements {
		switch n := en.(type) {
		case *stringNode:
			g.buf.Write(n.val)
		case *jsonPathNode:
			g.insertSegment(&jsonPathStringSegment{
				path: jp.MustParse(n.original),
			})
		case *variableNode:
			g.insertSegment(&variableStringSegment{name: n.name})
		default:
			panic("unexpected node type") // unreachable
		}
	}
	g.buf.WriteByte('"')
}

func (g *templateGenerator) insertSegment(segment templateSegment) {
	g.packLiteral()
	g.segments = append(g.segments, segment)
}

func (g *templateGenerator) packLiteral() {
	bs := g.buf.Bytes()
	if len(bs) != 0 {
		g.segments = append(g.segments, &literalSegment{
			val: append([]byte{}, bs...),
		})
		g.buf.Reset()
	}
}
