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
	"errors"

	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/trigger/util"
	"github.com/vanus-labs/vanus/observability/log"
)

type NodeType int

func (t NodeType) Type() NodeType {
	return t
}

const (
	Constant             NodeType = iota // Plain text.
	Define                               // A <var> variable, example "key":<var>
	DefineString                         // A <var> variable, example "key":"<var>" or "key":"other <var>"
	EventAttribute                       // A $.attributeName, example "key": $.id
	EventAttributeString                 // A $.attributeName, example "key": "$.id" or "key":"other $.id"
	EventData                            // A $.data.path, example "key": $.data.key
	EventDataString                      // A $.data.path, example "key": "$.data.key" or "key":"other $.data.key"
)

type Node interface {
	Type() NodeType
	Name() string
	Value(ctx *context.EventContext) (interface{}, bool)
}

type constantNode struct {
	NodeType
	text string
}

func (t *constantNode) Name() string {
	return t.text
}

func (p *parser) newConstant(text string) Node {
	return &constantNode{text: text, NodeType: Constant}
}

func (t *constantNode) Value(_ *context.EventContext) (interface{}, bool) {
	return t.text, true
}

type defineNode struct {
	NodeType
	name string
}

func newDefine(name string, valueIsStr bool) Node {
	if valueIsStr {
		return &defineNode{name: name, NodeType: DefineString}
	}
	return &defineNode{name: name, NodeType: Define}
}

func (t *defineNode) Name() string {
	return t.name
}

func (t *defineNode) Value(ceCtx *context.EventContext) (interface{}, bool) {
	v, exist := ceCtx.Define[t.name]
	return v, exist
}

type eventAttributeNode struct {
	NodeType
	attributeName string
}

// newEventAttributeNode name format $.id.
func newEventAttributeNode(name string, valueIsStr bool) Node {
	name = name[2:]
	if valueIsStr {
		return &eventAttributeNode{attributeName: name, NodeType: EventAttributeString}
	}
	return &eventAttributeNode{attributeName: name, NodeType: EventAttribute}
}

func (t *eventAttributeNode) Name() string {
	return EventArgPrefix + t.attributeName
}

func (t *eventAttributeNode) Value(ceCtx *context.EventContext) (interface{}, bool) {
	return util.LookupAttribute(*ceCtx.Event, t.attributeName)
}

type eventDataNode struct {
	NodeType
	path string
	data bool
}

// newEventDataNode name format: $.data.key.
func newEventDataNode(name string, valueIsStr bool) Node {
	var data bool
	var path string
	if name == EventDataArgPrefix {
		data = true
		path = ""
	} else {
		path = name[7:]
	}
	n := &eventDataNode{path: path, data: data}
	if valueIsStr {
		n.NodeType = EventDataString
	} else {
		n.NodeType = EventData
	}
	return n
}

func (t *eventDataNode) Name() string {
	if t.data {
		return EventDataArgPrefix
	}
	return EventDataArgPrefix + "." + t.path
}

func (t *eventDataNode) Value(ceCtx *context.EventContext) (interface{}, bool) {
	if t.data {
		return ceCtx.Data, true
	}
	v, err := util.LookupData(ceCtx.Data, EventArgPrefix+t.path)
	if err != nil {
		if errors.Is(err, util.ErrKeyNotFound) {
			return nil, false
		}
		log.Info().Err(err).
			Str("name", t.Name()).
			Msg("transformer template get data value error")
		return nil, false
	}
	return v, true
}
