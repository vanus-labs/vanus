package utils

import (
	"context"
	"github.com/linkall-labs/vanus/observability/log"
	"sync/atomic"
	"time"
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
				log.Info(ctx, "TPS printer is exit", map[string]interface{}{
					"notice": values,
				})
				return
			case <-timer.C:
				cur := map[string]int64{}
				val := map[string]interface{}{}
				for k, v := range values {
					cur[k] = atomic.LoadInt64(v)
					val[k] = cur[k] - prev[k]
				}
				log.Info(ctx, "TPS", val)
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
				log.Info(ctx, "Total printer is exit", nil)
				return
			case <-timer.C:
				cur := map[string]interface{}{}
				for k, v := range values {
					cur[k] = atomic.LoadInt64(v)
				}
				log.Info(ctx, "Total", cur)
			}
		}
	}()
}
