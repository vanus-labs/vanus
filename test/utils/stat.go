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

package utils

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/vanus-labs/vanus/observability/log"
)

func PrintTPS(ctx context.Context, values map[string]*int64) {
	go func() {
		timer := time.NewTicker(time.Second)
		prev := map[string]int64{}
		for k := range prev {
			prev[k] = 0
		}
		for {
			select {
			case <-ctx.Done():
				log.Info(ctx).Interface("notice", values).Msg("TPS printer is exit")
				return
			case <-timer.C:
				cur := map[string]int64{}
				val := map[string]interface{}{}
				for k, v := range values {
					cur[k] = atomic.LoadInt64(v)
					val[k] = cur[k] - prev[k]
				}
				log.Info(ctx).Interface("TPS", val).Msg("")
				prev = cur
			}
		}
	}()
}

func PrintTotal(ctx context.Context, values map[string]*int64) {
	go func() {
		timer := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				log.Info(ctx).Msg("Total printer is exit")
				return
			case <-timer.C:
				cur := map[string]interface{}{}
				for k, v := range values {
					cur[k] = atomic.LoadInt64(v)
				}
				log.Info(ctx).Interface("Total", cur).Msg("")
			}
		}
	}()
}
