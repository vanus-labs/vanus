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

package define

import (
	"fmt"

	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/transform/arg"
	"github.com/vanus-labs/vanus/pkg/transform/context"
)

type Define struct {
	args map[string]arg.Arg
}

func NewDefine() *Define {
	return &Define{
		args: map[string]arg.Arg{},
	}
}

func (d *Define) Parse(define map[string]string) {
	if len(define) == 0 {
		return
	}
	for key, value := range define {
		_arg, err := arg.NewArg(value)
		if err != nil {
			log.Warn().Err(err).
				Str("argName", value).Msg("arg is invalid")
			continue
		}
		d.args[key] = _arg
	}
}

func (d *Define) EvaluateValue(ceCtx *context.EventContext) (map[string]interface{}, error) {
	maps := make(map[string]interface{}, len(d.args))
	for k, v := range d.args {
		value, err := v.Evaluate(ceCtx)
		if err != nil {
			log.Warn().Err(err).
				Str("name", v.Original()).
				Str("type", fmt.Sprintf("%v", v.Type())).
				Msg("define var evaluate error")
		}
		maps[k] = value
	}
	return maps, nil
}
