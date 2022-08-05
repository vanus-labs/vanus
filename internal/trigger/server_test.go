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

package trigger

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"
	. "github.com/smartystreets/goconvey/convey"
)

func TestServerApi(t *testing.T) {
	Convey("test server api", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		w := NewMockWorker(ctrl)
		s := NewTriggerServer(Config{}).(*server)
		s.worker = w
		Convey("test start", func() {
			w.EXPECT().Start(gomock.Any()).Return(nil)
			_, err := s.Start(ctx, &pbtrigger.StartTriggerWorkerRequest{})
			So(err, ShouldBeNil)
			So(s.state, ShouldEqual, primitive.ServerStateRunning)
		})
		s.state = primitive.ServerStateRunning
		Convey("test add subscription", func() {
			w.EXPECT().AddSubscription(gomock.Any(), gomock.Any()).Return(nil)
			_, err := s.AddSubscription(ctx, &pbtrigger.AddSubscriptionRequest{})
			So(err, ShouldBeNil)
		})
		Convey("test add subscription has error", func() {
			w.EXPECT().AddSubscription(gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			_, err := s.AddSubscription(ctx, &pbtrigger.AddSubscriptionRequest{})
			So(err, ShouldNotBeNil)
		})
		Convey("test remove subscription", func() {
			w.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(nil)
			_, err := s.RemoveSubscription(ctx, &pbtrigger.RemoveSubscriptionRequest{})
			So(err, ShouldBeNil)
		})
		Convey("test remove subscription has error", func() {
			w.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			_, err := s.RemoveSubscription(ctx, &pbtrigger.RemoveSubscriptionRequest{})
			So(err, ShouldNotBeNil)
		})
		Convey("test pause subscription", func() {
			w.EXPECT().PauseSubscription(gomock.Any(), gomock.Any()).Return(nil)
			_, err := s.PauseSubscription(ctx, &pbtrigger.PauseSubscriptionRequest{})
			So(err, ShouldBeNil)
		})
		Convey("test pause subscription has error", func() {
			w.EXPECT().PauseSubscription(gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			_, err := s.PauseSubscription(ctx, &pbtrigger.PauseSubscriptionRequest{})
			So(err, ShouldNotBeNil)
		})
		Convey("test resume subscription", func() {
			w.EXPECT().StartSubscription(gomock.Any(), gomock.Any()).Return(nil)
			_, err := s.ResumeSubscription(ctx, &pbtrigger.ResumeSubscriptionRequest{})
			So(err, ShouldBeNil)
		})
		Convey("test resume subscription has error", func() {
			w.EXPECT().StartSubscription(gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			_, err := s.ResumeSubscription(ctx, &pbtrigger.ResumeSubscriptionRequest{})
			So(err, ShouldNotBeNil)
		})
		Convey("test reset offset to timestamp", func() {
			w.EXPECT().ResetOffsetToTimestamp(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			_, err := s.ResetOffsetToTimestamp(ctx, &pbtrigger.ResetOffsetToTimestampRequest{})
			So(err, ShouldBeNil)
		})
		Convey("test reset offset to timestamp has error", func() {
			w.EXPECT().ResetOffsetToTimestamp(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			_, err := s.ResetOffsetToTimestamp(ctx, &pbtrigger.ResetOffsetToTimestampRequest{})
			So(err, ShouldNotBeNil)
		})
	})
}

func TestServerInitAndClose(t *testing.T) {
	Convey("test server init and close", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		w := NewMockWorker(ctrl)
		s := NewTriggerServer(Config{}).(*server)
		s.worker = w
		Convey("test init", func() {
			w.EXPECT().Register(gomock.Any()).Return(nil)
			err := s.Initialize(ctx)
			So(err, ShouldBeNil)
			So(s.state, ShouldEqual, primitive.ServerStateStarted)
		})
		s.state = primitive.ServerStateRunning
		Convey("test stop", func() {
			w.EXPECT().Stop(gomock.Any()).Return(nil)
			w.EXPECT().Unregister(gomock.Any()).Return(nil)
			s.stop(ctx, true)
			So(s.state, ShouldEqual, primitive.ServerStateStopped)
		})
	})
}
