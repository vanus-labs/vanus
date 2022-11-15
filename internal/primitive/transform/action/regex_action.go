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

package action

import (
	"regexp"
	"sync"

	"github.com/linkall-labs/vanus/internal/primitive/transform/context"

	"github.com/linkall-labs/vanus/internal/primitive/transform/function"

	"github.com/pkg/errors"

	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
)

type replaceWithRegexAction struct {
	commonAction
	pattern *regexp.Regexp
	expr    string
	lock    sync.RWMutex
}

// ["replace_with_regex", "key", "pattern", "value"]
func newReplaceWithRegexAction() Action {
	return &replaceWithRegexAction{
		commonAction: commonAction{
			name:      "REPLACE_WITH_REGEX",
			fixedArgs: []arg.TypeList{arg.EventList, arg.All, arg.All},
		},
	}
}

func (a *replaceWithRegexAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args
	a.argTypes = []function.Type{function.String, function.String, function.String}
	return nil
}

func (a *replaceWithRegexAction) Execute(ceCtx *context.EventContext) error {
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	originalValue := args[0].(string)
	value := args[2].(string)
	expr := args[1].(string)
	if expr != a.expr {
		err = a.setPattern(expr)
		if err != nil {
			return err
		}
	}
	newValue := a.getPattern().ReplaceAllString(originalValue, value)
	return a.targetArg.SetValue(ceCtx, newValue)
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
