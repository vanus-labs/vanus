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

package leaderelection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestLeaderElection_NewLeaderElection(t *testing.T) {
	Convey("test leader election new", t, func() {
		c := &Config{
			LeaseDuration: 15,
			Name:          "timer",
			KeyPrefix:     "/vanus",
			EtcdEndpoints: []string{"127.0.0.1"},
		}
		Convey("test leader election new success", func() {
			stub1 := StubFunc(&newV3Client, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&newSession, nil, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&newMutex, nil)
			defer stub3.Reset()
			ret := NewLeaderElection(c)
			So(ret, ShouldNotBeNil)
		})
	})
}

func TestLeaderElection_Start(t *testing.T) {
	Convey("test leader election start", t, func() {
		ctx := context.Background()
		le := newleaderelection()
		mockCtrl := gomock.NewController(t)
		mutexMgr := NewMockMutex(mockCtrl)
		le.mutex = mutexMgr
		Convey("test leader election start success", func() {
			isLeader := false
			callbacks := LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).Times(1).Return(nil)
			err := le.Start(ctx, callbacks)
			So(err, ShouldBeNil)
			So(le.isLeader, ShouldEqual, true)
			So(isLeader, ShouldEqual, true)
		})
	})
}

func TestLeaderElection_Stop(t *testing.T) {
	Convey("test leader election stop", t, func() {
		ctx := context.Background()
		le := newleaderelection()
		mockCtrl := gomock.NewController(t)
		mutexMgr := NewMockMutex(mockCtrl)
		le.mutex = mutexMgr
		Convey("test leader election start success", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().Unlock(ctx).Times(1).Return(errors.New("test"))
			err := le.Stop(ctx)
			So(err, ShouldNotBeNil)
			So(le.isLeader, ShouldEqual, false)
			So(isLeader, ShouldEqual, false)
		})
	})
}

func TestLeaderElection_tryAcquireLockLoop(t *testing.T) {
	Convey("test leader election tryAcquireLockLoop", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		le := newleaderelection()
		mockCtrl := gomock.NewController(t)
		mutexMgr := NewMockMutex(mockCtrl)
		le.mutex = mutexMgr
		Convey("test leader election tryAcquireLockLoop success", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).Times(1).Return(nil)
			err := le.tryAcquireLockLoop(ctx)
			le.wg.Wait()
			So(err, ShouldBeNil)
			So(le.isLeader, ShouldEqual, true)
			So(isLeader, ShouldEqual, true)
		})

		Convey("test leader election tryAcquireLockLoop failure", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).AnyTimes().Return(concurrency.ErrLocked)
			go func() {
				time.Sleep(200 * time.Millisecond)
				cancel()
			}()
			err := le.tryAcquireLockLoop(ctx)
			le.wg.Wait()
			So(err, ShouldBeNil)
			So(le.isLeader, ShouldEqual, false)
			So(isLeader, ShouldEqual, false)
		})
	})
}

func TestLeaderElection_tryLock(t *testing.T) {
	Convey("test leader election tryLock", t, func() {
		ctx := context.Background()
		le := newleaderelection()
		mockCtrl := gomock.NewController(t)
		mutexMgr := NewMockMutex(mockCtrl)
		le.mutex = mutexMgr
		Convey("test leader election tryLock success", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).Times(1).Return(nil)
			err := le.tryLock(ctx)
			So(err, ShouldBeNil)
			So(le.isLeader, ShouldEqual, true)
			So(isLeader, ShouldEqual, true)
		})

		Convey("test leader election tryLock failure", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).Times(1).Return(concurrency.ErrLocked)
			err := le.tryLock(ctx)
			So(err, ShouldEqual, concurrency.ErrLocked)
			So(le.isLeader, ShouldEqual, false)
			So(isLeader, ShouldEqual, false)
		})

		Convey("test leader election tryLock abnormal", func() {
			isLeader := false
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) { isLeader = true },
				OnStoppedLeading: func(ctx context.Context) { isLeader = false },
			}
			mutexMgr.EXPECT().TryLock(ctx).Times(1).Return(errors.New("test"))
			err := le.tryLock(context.Background())
			So(err, ShouldNotBeNil)
			So(le.isLeader, ShouldEqual, false)
			So(isLeader, ShouldEqual, false)
		})
	})
}

func TestLeaderElection_release(t *testing.T) {
	Convey("test leader election release", t, func() {
		ctx := context.Background()
		le := newleaderelection()
		mockCtrl := gomock.NewController(t)
		mutexMgr := NewMockMutex(mockCtrl)
		le.mutex = mutexMgr
		Convey("test leader election release failure", func() {
			le.callbacks = LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {},
				OnStoppedLeading: func(ctx context.Context) {},
			}
			mutexMgr.EXPECT().Unlock(ctx).Times(1).Return(errors.New("test"))
			err := le.release(ctx)
			So(err, ShouldNotBeNil)
		})
	})
}

func newleaderelection() *leaderElection {
	return &leaderElection{
		name:          "timer",
		key:           "/vanus/timer",
		isLeader:      false,
		leaseDuration: 15,
	}
}
