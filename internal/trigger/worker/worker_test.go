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

package worker

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAddSubscription(t *testing.T) {
	Convey("add subscription", t, func() {
		ID := vanus.NewID()
		w := NewWorker(Config{Controllers: []string{"test"}})
		err := w.AddSubscription(&primitive.Subscription{
			ID: ID,
		})
		So(err, ShouldBeNil)
		_, exist := w.subscriptions[ID]
		So(exist, ShouldBeTrue)
		Convey("repeat add subscription", func() {
			err = w.AddSubscription(&primitive.Subscription{
				ID: ID,
			})
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, errors.ErrResourceAlreadyExist)
		})
	})
}

func TestListSubscriptionInfo(t *testing.T) {
	Convey("list subscription info", t, func() {
		ID := vanus.NewID()
		w := NewWorker(Config{Controllers: []string{"test"}})
		err := w.AddSubscription(&primitive.Subscription{
			ID: ID,
		})
		So(err, ShouldBeNil)
		list, f := w.ListSubscriptionInfo()
		f()
		So(len(list), ShouldEqual, 1)
		So(list[0].SubscriptionID, ShouldEqual, ID)
	})
}

//func TestRemoveSubscription(t *testing.T) {
//	Convey("remove subscription", t, func() {
//		ID := vanus.NewID()
//		w := NewWorker(Config{Controllers: []string{"test"}})
//		err := w.AddSubscription(&primitive.Subscription{
//			ID: ID,
//		})
//		So(err, ShouldBeNil)
//		ctx, cancel := context.WithCancel(context.Background())
//		go func() {
//			for {
//				select {
//				case <-ctx.Done():
//					return
//				default:
//					time.Sleep(time.Millisecond * 10)
//					_, f := w.ListSubscriptionInfo()
//					f()
//				}
//			}
//		}()
//		err = w.RemoveSubscription(ID)
//		cancel()
//		So(err, ShouldBeNil)
//		_, exist := w.subscriptions[ID]
//		So(exist, ShouldBeFalse)
//	})
//}

func TestPauseSubscription(t *testing.T) {
	Convey("pause subscription", t, func() {
		ID := vanus.NewID()
		w := NewWorker(Config{Controllers: []string{"test"}})
		err := w.AddSubscription(&primitive.Subscription{
			ID: ID,
		})
		So(err, ShouldBeNil)
		err = w.PauseSubscription(ID)
		So(err, ShouldBeNil)
		_, exist := w.subscriptions[ID]
		So(exist, ShouldBeTrue)
	})
}
