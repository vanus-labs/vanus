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

package arg

import (
	"errors"
	"strings"

	pkgUtil "github.com/vanus-labs/vanus/pkg/util"

	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/trigger/util"
)

type eventAttribute struct {
	attr     string
	original string
}

// newEventAttribute name format is $.source .
func newEventAttribute(name string) (Arg, error) {
	attr := strings.ToLower(name[2:])
	err := pkgUtil.ValidateEventAttrName(attr)
	if err != nil {
		return nil, err
	}
	return eventAttribute{
		attr:     attr,
		original: name,
	}, nil
}

func (arg eventAttribute) Type() Type {
	return EventAttribute
}

func (arg eventAttribute) Name() string {
	return arg.attr
}

func (arg eventAttribute) Original() string {
	return arg.original
}

func (arg eventAttribute) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	v, exist := util.LookupAttribute(*ceCtx.Event, arg.attr)
	if !exist {
		return nil, ErrArgValueNil
	}
	return v, nil
}

func (arg eventAttribute) SetValue(ceCtx *context.EventContext, value interface{}) error {
	return util.SetAttribute(ceCtx.Event, arg.attr, value)
}

func (arg eventAttribute) DeleteValue(ceCtx *context.EventContext) error {
	return util.DeleteAttribute(ceCtx.Event, arg.attr)
}

type eventData struct {
	path     string
	original string
}

// newEventData name format is $.data.key .
func newEventData(name string) Arg {
	if name == EventDataArgPrefix {
		return eventDataAll{
			eventData{
				path:     "",
				original: name,
			},
		}
	}
	return eventData{
		path:     name[7:],
		original: name,
	}
}

func (arg eventData) Type() Type {
	return EventData
}

func (arg eventData) Name() string {
	return arg.path
}

func (arg eventData) Original() string {
	return arg.original
}

func (arg eventData) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	v, err := util.LookupData(ceCtx.Data, EventArgPrefix+arg.path)
	if err != nil {
		if errors.Is(err, util.ErrKeyNotFound) {
			return nil, ErrArgValueNil
		}
		return nil, err
	}
	if v == nil {
		return nil, ErrArgValueNil
	}
	return v, nil
}

func (arg eventData) SetValue(ceCtx *context.EventContext, value interface{}) error {
	return util.SetData(ceCtx.Data, arg.path, value)
}

func (arg eventData) DeleteValue(ceCtx *context.EventContext) error {
	return util.DeleteData(ceCtx.Data, arg.path)
}

type eventDataAll struct {
	eventData
}

func (arg eventDataAll) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	return ceCtx.Data, nil
}

func (arg eventDataAll) SetValue(ceCtx *context.EventContext, value interface{}) error {
	ceCtx.Data = value
	return nil
}

func (arg eventDataAll) DeleteValue(ceCtx *context.EventContext) error {
	ceCtx.Data = map[string]interface{}{}
	return nil
}
