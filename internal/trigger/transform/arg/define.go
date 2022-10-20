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

import "github.com/linkall-labs/vanus/internal/trigger/context"

type define struct {
	name string
}

// newDefine name format is <var>
func newDefine(name string) Arg {
	return define{
		name: name[1 : len(name)-1],
	}
}

func (arg define) Type() Type {
	return Define
}

func (arg define) Name() string {
	return arg.name
}

func (arg define) Evaluate(ceCtx *context.EventContext) (interface{}, error) {
	if len(ceCtx.Define) == 0 {
		return nil, nil
	}
	v, exist := ceCtx.Define[arg.name]
	if !exist {
		return nil, nil
	}
	return v, nil
}
