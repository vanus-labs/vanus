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
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/observability/log"
)

type ceSQLFilter struct {
	rawExpression    string
	parsedExpression cesql.Expression
}

// NewCESQLFilter returns an event filter which passes if the provided CESQL expression
// evaluates.
func NewCESQLFilter(expr string) Filter {
	var parsed cesql.Expression
	var err error
	if expr != "" {
		parsed, err = cesqlparser.Parse(expr)
		if err != nil {
			log.Info("parse cesql filter expression error", map[string]interface{}{"expression": expr, "error": err})
			return nil
		}
	}
	return &ceSQLFilter{rawExpression: expr, parsedExpression: parsed}
}

func (filter *ceSQLFilter) Filter(event cloudevents.Event) FilterResult {
	if filter == nil || filter.rawExpression == "" {
		return NoFilter
	}
	log.Debug("cesql filter ", map[string]interface{}{"filters": filter, "event": event})
	res, err := filter.parsedExpression.Evaluate(event)

	if err != nil {
		log.Debug("cesql filter evaluate error ", map[string]interface{}{"filters": filter, "event": event})
		return FailFilter
	}

	if !res.(bool) {
		return FailFilter
	}
	return PassFilter
}
