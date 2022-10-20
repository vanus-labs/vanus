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
	"strings"

	"github.com/linkall-labs/vanus/internal/trigger/context"
	"github.com/linkall-labs/vanus/internal/trigger/util"
	pkgUtil "github.com/linkall-labs/vanus/pkg/util"
)

type eventAttribute struct {
	attr string
}

// newEventAttribute name format is $.source
func newEventAttribute(name string) (Arg, error) {
	attr := strings.ToLower(name[2:])
	err := pkgUtil.ValidateEventAttrName(attr)
	if err != nil {
		return nil, err
	}
	return eventAttribute{
		attr: attr,
	}, nil
}

func (arg eventAttribute) Type() Type {
	return EventAttribute
}
func (arg eventAttribute) Name() string {
	return arg.attr
}

func (arg eventAttribute) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	v, exist := util.LookupAttribute(*ceCtx.Event, arg.attr)
	if !exist {
		return nil, nil
	}
	return v, nil
}

type eventData struct {
	path string
}

// newEventData name format is $.data.key
func newEventData(name string) Arg {
	return eventData{
		path: name[7:],
	}
}

func (arg eventData) Type() Type {
	return EventData
}
func (arg eventData) Name() string {
	return arg.path
}

func (arg eventData) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	return util.LookupData(ceCtx.Data, "$."+arg.path)
}
