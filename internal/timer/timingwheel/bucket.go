package timingwheel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
	eventctl "github.com/linkall-labs/vanus/internal/timer/event"
	"github.com/linkall-labs/vanus/internal/timer/eventbus"
)

// Timer represents a single event. When the Timer expires, the given
// task will be executed.
type Timer struct {
	expiration int64 // in milliseconds
	event      *ce.Event
}

// func (t *Timer) getBucket() *bucket {
// 	return (*bucket)(atomic.LoadPointer(&t.b))
// }

// func (t *Timer) setBucket(b *bucket) {
// 	atomic.StorePointer(&t.b, unsafe.Pointer(b))
// }

// Stop prevents the Timer from firing. It returns true if the call
// stops the timer, false if the timer has already expired or been stopped.
//
// If the timer t has already expired and the t.task has been started in its own
// goroutine; Stop does not wait for t.task to complete before returning. If the caller
// needs to know whether t.task is completed, it must coordinate with t.task explicitly.
// func (t *Timer) Stop() bool {
// 	stopped := false
// 	for b := t.getBucket(); b != nil; b = t.getBucket() {
// 		// If b.Remove is called just after the timing wheel's goroutine has:
// 		//     1. removed t from b (through b.Flush -> b.remove)
// 		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
// 		// this may fail to remove t due to the change of t's bucket.
// 		stopped = b.Remove(t)

// 		// Thus, here we re-get t's possibly new bucket (nil for case 1, or ab (non-nil) for case 2),
// 		// and retry until the bucket becomes nil, which indicates that t has finally been removed.
// 	}
// 	return stopped
// }

type bucket struct {
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we must keep the 64-bit field
	// as the first field of the struct.
	//
	// For more explanations, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// and https://go101.org/article/memory-layout.html.
	// expiration int64
	mu       sync.Mutex
	offset   int64
	eventbus string
}

func newBucket(layer int64, slot int64) *bucket {
	eb := fmt.Sprintf("__Timer_%d_%d", layer, slot)
	err := eventbus.CreateEventBus(context.Background(), eb)
	if err != nil {
		panic(err)
	}
	return &bucket{
		eventbus: eb,
		// expiration: -1,
		offset: 0,
	}
}

// func (b *bucket) Expiration() int64 {
// 	return atomic.LoadInt64(&b.expiration)
// }

// func (b *bucket) SetExpiration(expiration int64) bool {
// 	return atomic.SwapInt64(&b.expiration, expiration) != expiration
// }

func (b *bucket) Add(t *Timer) {
	b.mu.Lock()
	eventctl.PutEvent(context.Background(), b.eventbus, t.event)
	b.mu.Unlock()
}

// func (b *bucket) remove(t *Timer) bool {
// 	if t.getBucket() != b {
// 		// If remove is called from within t.Stop, and this happens just after the timing wheel's goroutine has:
// 		//     1. removed t from b (through b.Flush -> b.remove)
// 		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
// 		// then t.getBucket will return nil for case 1, or ab (non-nil) for case 2.
// 		// In either case, the returned value does not equal to b.
// 		return false
// 	}
// 	b.timers.Remove(t.element)
// 	t.setBucket(nil)
// 	t.element = nil
// 	return true
// }

// func (b *bucket) Remove(t *Timer) bool {
// 	b.mu.Lock()
// 	defer b.mu.Unlock()
// 	return b.remove(t)
// }

func (b *bucket) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx := context.Background()
	for {
		events, err := eventctl.GetEvent(ctx, b.eventbus, b.offset, 1)
		if err != nil {
			if err.Error() != "on end" {
				log.Warning(ctx, "get event failed", map[string]interface{}{
					"eventbus":   b.eventbus,
					log.KeyError: err,
				})
			}
			return
		}

		if time.Now().After(events[0].Time()) {
			// todo： 当event的定时时间已到期，直接转发到真实的eventbus中
			// 更新etcd元数据信息
			// 更新offset
			log.Info(ctx, "get event success", map[string]interface{}{
				"event": events[0],
				"Time":  events[0].Time().Format("2006-01-02 15:04:05.000"),
			})
			b.offset++
			continue
		}
		break
	}
}

func (b *bucket) Load(reinsert func(*Timer) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ctx := context.Background()
	for {
		events, err := eventctl.GetEvent(ctx, b.eventbus, b.offset, 1)
		if err != nil {
			if err.Error() != "on end" {
				log.Warning(ctx, "get event failed", map[string]interface{}{
					"eventbus":   b.eventbus,
					log.KeyError: err,
				})
			}
			return
		}

		t := &Timer{
			expiration: events[0].Time().Unix(),
			event:      events[0],
		}
		// log.Info(context.Background(), "Load event", map[string]interface{}{
		// 	"t": t,
		// })
		if reinsert(t) {
			// todo： 当event降级成功后
			// 更新etcd元数据信息
			// 更新offset
			// 继续降级下一个event
			continue
		}
		break
	}
}
