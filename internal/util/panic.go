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

package util

import (
	"context"
	"github.com/linkall-labs/vanus/observability/log"
	"runtime"
)

func HandlePanic(customHandlers ...func(interface{})) {
	if r := recover(); r != nil {
		logPanic(r)
		for _, fn := range customHandlers {
			fn(r)
		}
	}
}

func logPanic(r interface{}) {
	size := 1 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
	log.Error(context.Background(), "get a panic", map[string]interface{}{"recover": r, "stacktrace": string(stacktrace)})
}
