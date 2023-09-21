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
	"runtime"

	cesql "github.com/cloudevents/sdk-go/sql/v2"
	cesqlparser "github.com/cloudevents/sdk-go/sql/v2/parser"
	ce "github.com/cloudevents/sdk-go/v2"

	"github.com/vanus-labs/vanus/pkg/observability/log"
)

type ceSQLFilter struct {
	rawExpression    string
	parsedExpression cesql.Expression
}

func NewCESQLFilter(expression string) Filter {
	if expression == "" {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			size := 1024
			stacktrace := make([]byte, size)
			stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
			log.Info().
				Str("expression", expression).
				Bytes("panic", stacktrace).
				Msg("parse cesql filter expression panic")
		}
	}()
	parsed, err := cesqlparser.Parse(expression)
	if err != nil {
		log.Info().Err(err).Str("expression", expression).Msg("parse cesql filter expression error")
		return nil
	}
	return &ceSQLFilter{rawExpression: expression, parsedExpression: parsed}
}

func (filter *ceSQLFilter) Filter(event ce.Event) Result {
	res, err := filter.parsedExpression.Evaluate(event)
	if err != nil {
		log.Info().
			Interface("filter", filter).
			Interface("event", event).
			Msg("cesql filter evaluate error ")
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
