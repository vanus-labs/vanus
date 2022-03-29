package ut

import (
	"github.com/golang/mock/gomock"
	"testing"
	"time"
)

func TestSpec1(t *testing.T) {
	ctrl := gomock.NewController(t)

	// Assert that Bar() is invoked.
	defer ctrl.Finish()

	m := NewMockFoo(ctrl)

	// Asserts that the first and only call to Bar() is passed 99.
	// Anything else will fail.
	m.
		EXPECT().
		Bar(99).
		Return(101)

	SUT(m)
}

func TestFoo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMockFoo(ctrl)

	// Does not make any assertions. Executes the anonymous functions and returns
	// its result when Bar is invoked with 99.
	m.
		EXPECT().
		Bar(gomock.Eq(99)).
		DoAndReturn(func(_ int) int {
			time.Sleep(1 * time.Second)
			return 101
		}).
		AnyTimes()

	// Does not make any assertions. Returns 103 when Bar is invoked with 101.
	m.
		EXPECT().
		Bar(gomock.Eq(101)).
		Return(103).
		AnyTimes()

	SUT(m)
}
