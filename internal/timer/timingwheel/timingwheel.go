package timingwheel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
)

// TimingWheel is an implementation of Hierarchical Timing Wheels.
type TimingWheel struct {
	tick        int64
	pointer     int64
	wheelSize   int64
	layer       int64
	layers      int64
	interval    int64
	currentTime int64
	buckets     []*bucket
	etcdClient  kv.Client

	// The higher-level overflow wheel.
	//
	// NOTE: This field may be updated and read concurrently, through Add().
	overflowWheel *TimingWheel // type: *TimingWheel

	leader bool
	exitC  chan struct{}
	// waitGroup waitGroupWrapper
	wg sync.WaitGroup
}

// NewTimingWheel creates an instance of TimingWheel with the given tick and wheelSize.
// func NewTimingWheel(tick time.Duration, wheelSize int64, layers int64) *TimingWheel {
func NewTimingWheel(c *Config) *TimingWheel {
	if c.Tick <= 0 {
		panic(errors.New("tick must be greater than or equal to 1s"))
	}

	now := time.Now()
	startSec := timeToSec(now.UTC())
	fmt.Printf("startSec: %d, %s\n", startSec, now.Format("2006-01-02 15:04:05.000"))

	return newTimingWheel(
		c.EtcdClient,
		c.Tick,
		c.WheelSize,
		1,
		c.Layers,
		startSec,
	)
}

// newTimingWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimingWheel(client kv.Client, tickSec int64, wheelSize int64, layer int64, layers int64, startSec int64) *TimingWheel {
	var buckets []*bucket
	if layer > layers {
		buckets = make([]*bucket, 0)
	} else {
		buckets = make([]*bucket, wheelSize)
		for i := range buckets {
			buckets[i] = newBucket(layer, int64(i))
		}
	}

	var overflowWheel *TimingWheel = nil
	if layer <= layers {
		overflowWheel = newTimingWheel(
			client,
			tickSec*wheelSize,
			wheelSize,
			layer+1,
			layers,
			startSec,
		)
	}

	return &TimingWheel{
		tick:          tickSec,
		pointer:       0,
		wheelSize:     wheelSize,
		layer:         layer,
		layers:        layers,
		etcdClient:    client,
		currentTime:   startSec,
		interval:      tickSec * wheelSize,
		buckets:       buckets,
		overflowWheel: overflowWheel,
		leader:        false,
		exitC:         make(chan struct{}),
	}
}

func (tw *TimingWheel) SetLeader(isleader bool) {
	if tw.layer > tw.layers {
		return
	}
	tw.leader = isleader
	tw.overflowWheel.SetLeader(isleader)
}

func (tw *TimingWheel) isLeader() bool {
	return tw.leader
}

// func setcap(b []*bucket, cap int) []*bucket {
// 	s := make([]*bucket, cap)
// 	copy(s, b)
// 	return s
// }

func (tw *TimingWheel) Add(e *ce.Event) *Timer {
	t := &Timer{
		expiration: timeToSec(e.Time()),
		event:      e,
	}

	// if !tw.isLeader() {
	// 	return nil
	// }

	for {
		if tw.isLeader() {
			break
		}
		time.Sleep(1 * time.Second)
	}

	tw.add(t)
	return t
}

// add inserts the timer t into the current timing wheel.
func (tw *TimingWheel) add(t *Timer) bool {
	// currentTime := atomic.LoadInt64(&tw.currentTime)
	currentTime := timeToSec(time.Now().UTC())
	// fmt.Printf("add layer[%d] tick[%d] interval[%d] expiration[%d] currentTime[%d]\n", tw.layer, tw.tick, tw.interval, t.expiration, currentTime)
	if (tw.layer == 1) && (t.expiration <= currentTime) {
		// Already expired
		// 直接转发到真实的eventbus中
		return true
	} else if t.expiration < currentTime+tw.interval {
		// Put it into its own bucket
		virtualID := (t.expiration - currentTime) / tw.tick

		// fmt.Printf("task[%d] insert layer[%d] index: %d\n", t.id, tw.layer, (t.expiration-currentTime)/tw.tick)
		b := tw.buckets[(tw.pointer+virtualID)%tw.wheelSize]
		// b := tw.buckets[(tw.pointer+virtualID)%tw.wheelSize]
		log.Info(context.Background(), "add event to eventbus", map[string]interface{}{
			"eventbus":    b.eventbus,
			"layer":       tw.layer,
			"index":       (tw.pointer + virtualID) % tw.wheelSize,
			"currentTime": time.Now().Format("2006-01-02 15:04:05.000"),
			"pointer":     tw.pointer,
		})
		b.Add(t)

		return true
	} else {
		if tw.layer <= tw.layers {
			// Out of the interval. Put it into the overflow wheel
			return tw.overflowWheel.add(t)
		} else {
			// Put it into its own bucket
			virtualID := t.expiration / tw.tick
			if cap(tw.buckets) < int(virtualID) {
				tw.buckets = setcap(tw.buckets, int(virtualID))
			}
			if tw.buckets[virtualID] == nil {
				tw.buckets[virtualID] = newBucket(tw.layer, virtualID)
			}

			b := tw.buckets[virtualID]
			b.Add(t)
			return true
		}
	}
}

func (tw *TimingWheel) load(reinsert func(*Timer) bool) {
	endPointer := (tw.pointer + 2) % tw.wheelSize
	index := (tw.pointer + 1) % tw.wheelSize
	// todo: 最高级时间轮降级前需要先判空，避免数组越界panic
	if len(tw.buckets) < int(index)+1 {
		return
	}
	// log.Info(context.Background(), "load event", map[string]interface{}{
	// 	"tw": tw,
	// })
	for {
		if tw.pointer == endPointer {
			return
		}
		tw.buckets[index].Load(reinsert)
		time.Sleep(100 * time.Millisecond)
		// time.Sleep(time.Duration(tw.tick / tw.wheelSize))
	}
	// todo: 最高级时间轮降级完成后需要将当前index的bucket清理，避免数据越来越长
	// 这时候数组这种数据结构获取不太好清理，是否需要换成链表
	// 或者可以考虑仅记录一个offset，再下一次最高层数据扩容是再进行清理
}

func (tw *TimingWheel) startConsumeRoutine(ctx context.Context) error {
	if tw.layer != 1 {
		return nil
	}
	for i := int64(0); i < tw.wheelSize; i++ {
		tw.wg.Add(1)
		go func(index int64) {
			for {
				select {
				case <-ctx.Done():
					tw.wg.Done()
					return
				default:
					if tw.isLeader() {
						tw.buckets[index].Flush()
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(i)
	}
	return nil
}

func (tw *TimingWheel) startPointer(ctx context.Context) error {
	start := atomic.LoadInt64(&tw.currentTime)
	tw.wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				tw.wg.Done()
				return
			default:
				now := timeToSec(time.Now().UTC())
				// todo: le.observedRecord.RenewTime.Add(le.config.LeaseDuration).After(time.Now())
				if (now - start) >= tw.tick {
					tw.pointer = (tw.pointer + 1) % tw.wheelSize
					// fmt.Printf("===TimingWheel[%d]=== pointer[%d] time[%s]\n", tw.layer, tw.pointer, time.Now().Format("2006-01-02 15:04:05.000"))
					start = now
					if tw.isLeader() && tw.pointer == (tw.wheelSize-1) {
						go tw.overflowWheel.load(tw.add)
					}
				}
				time.Sleep(time.Duration(tw.tick / 50))
			}
		}
	}()
	return nil
}

// Start starts the current timing wheel.
func (tw *TimingWheel) Start(ctx context.Context) error {

	// start the bottom timingwheel consumption routine
	tw.startConsumeRoutine(ctx)

	// start the timingwheel pointer of each layer
	tw.startPointer(ctx)

	if tw.layer == tw.layers {
		return nil
	}

	// start overflowWheel
	go tw.overflowWheel.Start(ctx)
	return nil
}

// Stop stops the current timing wheel.
//
// If there is any timer's task being running in its own goroutine, Stop does
// not wait for the task to complete before returning. If the caller needs to
// know whether the task is completed, it must coordinate with the task explicitly.
func (tw *TimingWheel) Stop() {
	close(tw.exitC)
	tw.wg.Wait()
}
