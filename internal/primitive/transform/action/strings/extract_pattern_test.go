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

 package strings

 import (
 	"testing"

 	"github.com/stretchr/testify/assert"
 	"github.com/vanus-labs/vanus/internal/primitive/transform/action"
 	"github.com/vanus-labs/vanus/internal/primitive/transform/arg"
 	"github.com/vanus-labs/vanus/internal/primitive/transform/common"
 	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
 )

 func TestExtractPatternAction(t *testing.T) {
 	a := NewExtractPatternAction()
 	assert.Equal(t, "EXTRACT_PATTERN", a.Name())
 	assert.Equal(t, []arg.TypeList{
 		arg.EventList,
 		arg.EventList,
 		[]arg.Type{arg.Constant},
 		[]arg.Type{arg.Constant},
 	}, a.FixedArgs())


 	ceCtx := context.NewEventContext()
 	ceCtx.Set(arg.EventList[0], "a*")
 	err := a.Init([]arg.Arg{
 		arg.EventList[0],
 		arg.EventList[1],
 		arg.NewConstantArg("trueFlagReplacement"),
 		arg.NewConstantArg("falseFlagReplacement"),
 	})
 	assert.Nil(t, err)
 	assert.Equal(t, []arg.Arg{arg.EventList[0], arg.NewConstantArg("trueFlagReplacement"), arg.NewConstantArg("falseFlagReplacement")}, a.Args)
 	assert.Equal(t, []common.Type{common.String, common.Any, common.Any}, a.ArgTypes)
 	assert.Equal(t, arg.EventList[1], a.TargetArg)
 	err = a.Execute(ceCtx)
 	assert.Nil(t, err)
 	assert.Equal(t, "trueFlagReplacement", ceCtx.Get(arg.EventList[1]))
 }

 func TestExtractPatternAction2(t *testing.T) {
 	a := NewExtractPatternAction()
 	assert.Equal(t, "EXTRACT_PATTERN", a.Name())
 	assert.Equal(t, []arg.TypeList{
 		arg.EventList,
 		[]arg.Type{arg.Constant},


 	}, a.FixedArgs())
 	ceCtx := context.NewEventContext()
 	ceCtx.Set(arg.EventList[0], "b*")
 	err := a.Init([]arg.Arg{
 		arg.EventList[0],
 		arg.EventList[1],
 		arg.NewConstantArg("trueFlagReplacement"),
 		arg.NewConstantArg("falseFlagReplacement"),
 	})
 	assert.Nil(t, err)

 	assert.Equal(t, []arg.Arg{arg.EventList[0], arg.NewConstantArg("trueFlagReplacement"), arg.NewConstantArg("falseFlagReplacement")}, a.Args)
 	assert.Equal(t, []common.Type{common.String, common.Any, common.Any}, a.ArgTypes)
 	assert.Equal(t, arg.EventList[1], a.TargetArg)
 	err = a.Execute(ceCtx)
 	assert.Nil(t, err)
 	assert.Equal(t, "falseFlagReplacement", ceCtx.Get(arg.EventList[1]))
 }

