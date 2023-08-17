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

package datetime

import (
	"time"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	// "github.com/vanus-labs/vanus/internal/primitive/transform/function"
)

type todayAction struct {
	action.CommonAction
}

// NewTodayAction [ "targetJsonPath", "TimeZone"].
func NewTodayAction() action.Action {
	return &todayAction{
		CommonAction: action.CommonAction{
			ActionName:  "TODAY",
			FixedArgs:   []arg.TypeList{arg.EventList},
			VariadicArg: arg.TypeList{arg.Constant},
		},
	}
}

func (a *todayAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	if len(args) == 1 {
		utc, err := arg.NewArg(time.UTC.String())
		if err != nil {
			return err
		}
		args = append(args, utc)
	}
	a.Args = args[1:]
	a.ArgTypes = []common.Type{common.String}
	return nil
}

func (a *todayAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}

	loc := new(time.Location)
	if len(args) > 0 && args[0].(string) != "" {
		var err error
		loc, err = time.LoadLocation(args[0].(string))
		if err != nil {
			return err
		}
	}
	t := time.Now()
	today := t.In(loc).Format(time.DateOnly)

	return a.TargetArg.SetValue(ceCtx, today)
}
