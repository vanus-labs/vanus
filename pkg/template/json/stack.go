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

type parserContext struct {
	node  templateNode
	state parserState
}

type parserStack struct {
	stack []parserContext
}

func (ps *parserStack) push(n templateNode, s parserState) {
	ps.stack = append(ps.stack, parserContext{node: n, state: s})
}

func (ps *parserStack) pop() (templateNode, parserState) {
	i := len(ps.stack) - 1
	pc := ps.stack[i]
	ps.stack = ps.stack[:i]
	return pc.node, pc.state
}

func (ps *parserStack) peek() (templateNode, parserState) {
	pc := ps.stack[len(ps.stack)-1]
	return pc.node, pc.state
}

type generatorContext struct {
	node templateNode
	iter int
}

type generatorStack struct {
	stack []generatorContext
}

func (gs *generatorStack) push(n templateNode) {
	gs.stack = append(gs.stack, generatorContext{node: n, iter: 0})
}

func (gs *generatorStack) advance() {
	gs.stack[len(gs.stack)-1].iter++
}

func (gs *generatorStack) advanceThenPush(n templateNode) {
	gs.advance()
	gs.push(n)
}

func (gs *generatorStack) pop() bool {
	i := len(gs.stack) - 1
	gs.stack = gs.stack[:i]
	return i == 0
}

func (gs *generatorStack) peek() (templateNode, int) {
	gc := &gs.stack[len(gs.stack)-1]
	return gc.node, gc.iter
}
