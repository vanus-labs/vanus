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

package define

import (
	"strings"
)

type Type int

const (
	Constant Type = iota
	ContextVariable
	DataVariable
)

type Node struct {
	Type  Type
	Key   string
	Value string
}

type Parser struct {
	nodes           map[string]Node
	hasDataVariable bool
}

func NewParse() *Parser {
	return &Parser{
		nodes: map[string]Node{},
	}
}

func (p *Parser) HasDataVariable() bool {
	return p.hasDataVariable
}

func (p *Parser) GetNodes() map[string]Node {
	return p.nodes
}

func (p *Parser) GetNode(keyName string) (Node, bool) {
	n, exist := p.nodes[keyName]
	return n, exist
}

func (p *Parser) Parse(input map[string]string) {
	if len(input) == 0 {
		return
	}
	nodeMap := make(map[string]Node, len(input))
	for k, v := range input {
		if len(v) <= 2 || v[:2] != "$." {
			nodeMap[k] = Node{Type: Constant, Key: k, Value: v}
			continue
		}

		keys := strings.Split(v[2:], ".")
		if keys[0] == "data" {
			if len(keys) == 1 {
				nodeMap[k] = Node{Type: DataVariable, Key: k}
			} else {
				p.hasDataVariable = true
				nodeMap[k] = Node{Type: DataVariable, Key: k, Value: v[7:]}
			}
		} else {
			nodeMap[k] = Node{Type: ContextVariable, Key: k, Value: v[2:]}
		}
	}
	p.nodes = nodeMap
}
