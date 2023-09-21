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

package filter

import (
	ce "github.com/cloudevents/sdk-go/v2"

	"github.com/vanus-labs/vanus/pkg/cel"
	"github.com/vanus-labs/vanus/pkg/observability/log"
)

type CELFilter struct {
	rawExpression    string
	parsedExpression *cel.Expression
}

func NewCELFilter(expression string) Filter {
	if expression == "" {
		return nil
	}
	cel, err := cel.Parse(expression)
	if err != nil {
		log.Info().Err(err).Str("expression", expression).Msg("parse cel expression error")
		return nil
	}
	return &CELFilter{rawExpression: expression, parsedExpression: cel}
}

func (filter *CELFilter) Filter(event ce.Event) Result {
	result, err := filter.parsedExpression.Eval(event)
	if err != nil {
		log.Info().Err(err).Msg("cel eval error")
		return FailFilter
	}
	if result {
		return PassFilter
	}
	return FailFilter
}

func (filter *CELFilter) String() string {
	return filter.rawExpression
}
