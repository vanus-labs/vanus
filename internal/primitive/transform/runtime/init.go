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

package runtime

import (
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/condition"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/datetime"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/math"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/render"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/structs"
)

func init() {
	for _, fn := range []newAction{
		// struct
		structs.NewCreateAction,
		structs.NewDeleteAction,
		structs.NewReplaceAction,
		structs.NewMoveAction,
		structs.NewRenameAction,
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
		// condition
		condition.NewConditionIfAction,
		// render
		render.NewRenderArrayAction,
	} {
		if err := AddAction(fn); err != nil {
			panic(err)
		}
	}
}
