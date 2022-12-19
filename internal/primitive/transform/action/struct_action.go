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
	"fmt"

	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
)

// ["delete", "key"].
type deleteAction struct {
	commonAction
}

func newDeleteAction() Action {
	return &deleteAction{
		commonAction{
			name:      "DELETE",
			fixedArgs: []arg.TypeList{arg.EventList},
		},
	}
}

func (a *deleteAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	return nil
}

func (a *deleteAction) Execute(ceCtx *context.EventContext) error {
	return a.targetArg.DeleteValue(ceCtx)
}

type createAction struct {
	commonAction
}

// ["create", "toKey", value].
func newCreateActionAction() Action {
	return &createAction{
		commonAction{
			name:      "CREATE",
			fixedArgs: []arg.TypeList{arg.EventList, arg.All},
		},
	}
}

func (a *createAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args[1:]
	a.argTypes = []common.Type{common.Any}
	return nil
}

func (a *createAction) Execute(ceCtx *context.EventContext) error {
	v, _ := a.targetArg.Evaluate(ceCtx)
	if v != nil {
		return fmt.Errorf("key %s exist", a.targetArg.Original())
	}
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	return a.targetArg.SetValue(ceCtx, args[0])
}

type replaceAction struct {
	commonAction
}

// ["replace", "toKey", value].
func newReplaceAction() Action {
	return &replaceAction{
		commonAction{
			name:      "REPLACE",
			fixedArgs: []arg.TypeList{arg.EventList, arg.All},
		},
	}
}

func (a *replaceAction) Init(args []arg.Arg) error {
	a.targetArg = args[0]
	a.args = args[1:]
	a.argTypes = []common.Type{common.Any}
	return nil
}

func (a *replaceAction) Execute(ceCtx *context.EventContext) error {
	v, _ := a.targetArg.Evaluate(ceCtx)
	if v == nil {
		return fmt.Errorf("key %s not exist", a.targetArg.Original())
	}
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	return a.targetArg.SetValue(ceCtx, args[0])
}

type moveAction struct {
	commonAction
}

// ["move", "fromKey", "toKey"].
func newMoveActionAction() Action {
	return &moveAction{
		commonAction{
			name:      "MOVE",
			fixedArgs: []arg.TypeList{arg.EventList, arg.EventList},
		},
	}
}

func (a *moveAction) Init(args []arg.Arg) error {
	a.targetArg = args[1]
	a.args = args[:1]
	a.argTypes = []common.Type{common.Any}
	return nil
}

func (a *moveAction) Execute(ceCtx *context.EventContext) error {
	v, _ := a.targetArg.Evaluate(ceCtx)
	if v != nil {
		return fmt.Errorf("key %s exist", a.targetArg.Original())
	}
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	err = a.targetArg.SetValue(ceCtx, args[0])
	if err != nil {
		return err
	}
	return a.args[0].DeleteValue(ceCtx)
}

type renameAction struct {
	commonAction
}

// ["rename", "key", "newKey"].
func newRenameActionAction() Action {
	return &renameAction{
		commonAction{
			name:      "RENAME",
			fixedArgs: []arg.TypeList{arg.EventList, arg.EventList},
		},
	}
}

func (a *renameAction) Init(args []arg.Arg) error {
	a.targetArg = args[1]
	a.args = args[:1]
	a.argTypes = []common.Type{common.Any}
	return nil
}

func (a *renameAction) Execute(ceCtx *context.EventContext) error {
	v, _ := a.targetArg.Evaluate(ceCtx)
	if v != nil {
		return fmt.Errorf("key %s exist", a.targetArg.Original())
	}
	args, err := a.runArgs(ceCtx)
	if err != nil {
		return err
	}
	err = a.targetArg.SetValue(ceCtx, args[0])
	if err != nil {
		return err
	}
	return a.args[0].DeleteValue(ceCtx)
}
