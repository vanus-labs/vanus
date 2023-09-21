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

package strings

import (
	"regexp"
	"sync"

	"github.com/pkg/errors"

	"github.com/vanus-labs/vanus/pkg/transform/action"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/common"
	"github.com/vanus-labs/vanus/pkg/transform/context"
)

type replaceWithRegexAction struct {
	action.CommonAction
	pattern *regexp.Regexp
	expr    string
	lock    sync.RWMutex
}

// NewReplaceWithRegexAction ["replace_with_regex", "key", "pattern", "value"].
func NewReplaceWithRegexAction() action.Action {
	return &replaceWithRegexAction{
		CommonAction: action.CommonAction{
			ActionName: "REPLACE_WITH_REGEX",
			FixedArgs:  []arg.TypeList{arg.EventList, arg.All, arg.All},
		},
	}
}

func (a *replaceWithRegexAction) Init(args []arg.Arg) error {
	a.TargetArg = args[0]
	a.Args = args
	a.ArgTypes = []common.Type{common.String, common.String, common.String}
	return nil
}

func (a *replaceWithRegexAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.RunArgs(ceCtx)
	if err != nil {
		return err
	}
	originalValue, _ := args[0].(string)
	value, _ := args[2].(string)
	expr, _ := args[1].(string)
	if expr != a.expr {
		err = a.setPattern(expr)
		if err != nil {
			return err
		}
	}
	newValue := a.getPattern().ReplaceAllString(originalValue, value)
	return a.TargetArg.SetValue(ceCtx, newValue)
}

func (a *replaceWithRegexAction) setPattern(expr string) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	if expr == a.expr {
		return nil
	}
	p, err := regexp.Compile(expr)
	if err != nil {
		a.lock.Unlock()
		return errors.Wrapf(err, "replace_with_regex arg pattern regex invalid")
	}
	a.pattern = p
	a.expr = expr
	return nil
}

func (a *replaceWithRegexAction) getPattern() *regexp.Regexp {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.pattern
}
