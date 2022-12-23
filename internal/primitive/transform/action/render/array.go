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

package render

import (
	"fmt"
	"strings"

	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

// ["render_array","targetPath","render root","render template"]
type renderArrayAction struct {
	action.CommonAction
	paths      []string
	template   string
	leftDelim  string
	rightDelim string
}

func NewRenderArrayAction() action.Action {
	a := &renderArrayAction{
		leftDelim:  "<@",
		rightDelim: ">",
	}
	a.CommonAction = action.CommonAction{
		ActionName: "RENDER_ARRAY",
		FixedArgs:  []arg.TypeList{arg.EventList, []arg.Type{arg.EventData}, []arg.Type{arg.Constant}},
	}
	return a
}

func (a *renderArrayAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	jsonPrefix := args[1].Original()
	text := args[2].Original()
	var pos int
	var sb strings.Builder
	leftDelimLen := len(a.leftDelim)
	for {
		x := strings.Index(text[pos:], a.leftDelim)
		if x < 0 {
			sb.WriteString(text[pos:])
			break
		}
		ldp := pos + x + leftDelimLen
		y := strings.Index(text[ldp:], a.rightDelim)
		if y < 0 {
			sb.WriteString(text[pos:])
			break
		}
		if x > 0 {
			sb.WriteString(text[pos : pos+x])
		}
		a.paths = append(a.paths, text[ldp:ldp+y])

		sb.WriteString("%v")
		pos = ldp + y + len(a.rightDelim)
		if pos == len(text) {
			break
		}
	}
	a.template = sb.String()
	for _, path := range a.paths {
		_arg, err := arg.NewArg(jsonPrefix + "[:]" + path)
		if err != nil {
			return err
		}
		a.Args = append(a.Args, _arg)
		a.ArgTypes = append(a.ArgTypes, common.Array)
	}
	return nil
}

func (a *renderArrayAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return a.TargetArg.SetValue(ceCtx, []string{a.template})
	}
	_len := len(args[0].([]interface{}))
	for i := 1; i < len(args); i++ {
		if len(args[i].([]interface{})) != _len {
			return fmt.Errorf("template %s value length is not same", a.leftDelim+a.paths[i]+a.rightDelim)
		}
	}

	target := make([]string, _len)
	for i := 0; i < _len; i++ {
		value := make([]interface{}, len(a.paths))
		for j := 0; j < len(args); j++ {
			v := args[j].([]interface{})
			value[j] = v[i]
		}
		target[i] = fmt.Sprintf(a.template, value...)
	}
	return a.TargetArg.SetValue(ceCtx, target)
}
