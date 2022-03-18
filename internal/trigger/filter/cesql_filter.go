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
	cesql "github.com/cloudevents/sdk-go/sql/v2"
	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/observability/log"
)

type ceSQLFilter struct {
	rawExpression    string
	parsedExpression cesql.Expression
}

func NewCESQLFilter(expression string) Filter {
	if expression == "" {
		return nil
	}
	parsed, err := cesqlparser.Parse(expression)
	if err != nil {
		log.Info(ctx, "parse cesql filter expression error", map[string]interface{}{"expression": expression, "error": err})
		return nil
	}
	return &ceSQLFilter{rawExpression: expression, parsedExpression: parsed}
}

func (filter *ceSQLFilter) Filter(event ce.Event) FilterResult {
	if filter == nil {
		return FailFilter
	}
	log.Debug(ctx, "cesql filter ", map[string]interface{}{"filter": filter, "event": event})
	res, err := filter.parsedExpression.Evaluate(event)
	if err != nil {
		log.Info(ctx, "cesql filter evaluate error ", map[string]interface{}{"filter": filter, "event": event})
		return FailFilter
	}

	if !res.(bool) {
		return FailFilter
	}
	return PassFilter
}

func (filter *ceSQLFilter) String() string {
	return filter.rawExpression
}
