package timingwheel

import (
	"sync"
	"time"
)

// truncate returns the result of rounding x toward zero to a multiple of m.
// If m <= 0, Truncate returns x unchanged.
func truncate(x, m int64) int64 {
	if m <= 0 {
		return x
	}
	return x - x%m
}

func timeToSec(t time.Time) int64 {
	return t.UnixNano() / int64(time.Second)
}

// timeToMs returns an integer number, which represents t in milliseconds.
func timeToMs(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// msToTime returns the UTC time corresponding to the given Unix time,
// t milliseconds since January 1, 1970 UTC.
func msToTime(t int64) time.Time {
	return time.Unix(0, t*int64(time.Millisecond)).UTC()
}

type waitGroupWrapper struct {
	sync.WaitGroup
}

func (w *waitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

func setcap(b []*bucket, cap int) []*bucket {
	s := make([]*bucket, cap)
	copy(s, b)
	return s
}

func exponent(m, n int64) int64 {
	result := int64(1)
	for i := n; i > 0; i >>= 1 {
		if i&1 != 0 {
			result *= m
		}
		m *= m
	}
	return result
}
