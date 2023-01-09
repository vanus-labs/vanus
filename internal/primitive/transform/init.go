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

package transform

import (
	"github.com/linkall-labs/vanus/internal/primitive/transform/action"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/array"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/common"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/condition"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/datetime"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/math"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/source"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/structs"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
)

func init() {
	for _, fn := range []func() action.Action{
		// struct
		structs.NewCreateAction,
		structs.NewDeleteAction,
		structs.NewReplaceAction,
		structs.NewMoveAction,
		structs.NewRenameAction,
		structs.NewDuplicateAction,
		// math
		math.NewMathAddAction,
		math.NewMathSubAction,
		math.NewMathMulAction,
		math.NewMathDivAction,
		// datetime
		datetime.NewDateFormatAction,
		datetime.NewUnixTimeFormatAction,
		// string
		strings.NewJoinAction,
		strings.NewUpperAction,
		strings.NewLowerAction,
		strings.NewAddPrefixAction,
		strings.NewAddSuffixAction,
		strings.NewReplaceWithRegexAction,
		strings.NewReplaceStringAction,
		strings.NewReplaceBetweenPositionsAction,
		// condition
		condition.NewConditionIfAction,
		// render
		array.NewRenderArrayAction,
		array.NewForeachArrayAction,
		// common
		common.NewLengthAction,
		// source
		source.NewDebeziumConvertToMongoDBSink,
	} {
		if err := runtime.AddAction(fn); err != nil {
			panic(err)
		}
	}
}