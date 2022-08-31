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
	stdJson "encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/controller/trigger/worker"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestController_CommitOffset(t *testing.T) {
	Convey("test reset offset", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager

		subID := vanus.NewID()
		ctrl.state = primitive.ServerStateRunning
		request := &ctrlpb.CommitOffsetRequest{
			ForceCommit: true,
			SubscriptionInfo: []*metapb.SubscriptionInfo{{
				SubscriptionId: subID.Uint64(),
				Offsets: []*metapb.OffsetInfo{{
					EventLogId: vanus.NewID().Uint64(),
					Offset:     100,
				}},
			}},
		}
		Convey("commit offset fail", func() {
			subManager.EXPECT().SaveOffset(gomock.Any(), gomock.Eq(subID), gomock.Any(), gomock.Any()).Return(fmt.Errorf("test error"))
			resp, err := ctrl.CommitOffset(ctx, request)
			So(err, ShouldBeNil)
			So(len(resp.FailSubscriptionId), ShouldEqual, 1)
			So(resp.FailSubscriptionId[0], ShouldEqual, subID)
		})
		Convey("commit offset success", func() {
			subManager.EXPECT().SaveOffset(gomock.Any(), gomock.Eq(subID), gomock.Any(), gomock.Any()).Return(nil)
			resp, err := ctrl.CommitOffset(ctx, request)
			So(err, ShouldBeNil)
			So(len(resp.FailSubscriptionId), ShouldEqual, 0)
		})
	})
}

func TestController_ResetOffsetToTimestamp(t *testing.T) {
	Convey("test reset offset", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager

		addr := "test"
		subID := vanus.NewID()
		ctrl.state = primitive.ServerStateRunning
		Convey("reset offset subscription not exist", func() {
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).AnyTimes().Return(nil)
			_, err := ctrl.ResetOffsetToTimestamp(ctx, &ctrlpb.ResetOffsetToTimestampRequest{
				SubscriptionId: subID.Uint64(),
				Timestamp:      uint64(time.Now().Unix()),
			})
			So(err, ShouldNotBeNil)
		})
		Convey("reset offset subscription exist", func() {
			sub := &metadata.Subscription{
				ID:            subID,
				Phase:         metadata.SubscriptionPhaseRunning,
				TriggerWorker: addr,
			}
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).AnyTimes().Return(sub)
			tWorker := worker.NewMockTriggerWorker(mockCtrl)
			tWorker.EXPECT().ResetOffsetToTimestamp(gomock.Eq(subID), gomock.Any()).Return(nil)
			workerManager.EXPECT().GetTriggerWorker(addr).Return(tWorker)
			_, err := ctrl.ResetOffsetToTimestamp(ctx, &ctrlpb.ResetOffsetToTimestampRequest{
				SubscriptionId: subID.Uint64(),
				Timestamp:      uint64(time.Now().Unix()),
			})
			So(err, ShouldBeNil)
		})
	})
}

func TestController_CreateSubscription(t *testing.T) {
	Convey("test create subscription", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)

		ctrl.state = primitive.ServerStateRunning
		Convey("create subscription", func() {
			subManager.EXPECT().AddSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			create := &ctrlpb.CreateSubscriptionRequest{
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-bus",
					Sink:     "test-sink",
				},
			}
			request := create.Subscription
			resp, err := ctrl.CreateSubscription(ctx, create)
			So(err, ShouldBeNil)
			So(resp.Sink, ShouldEqual, request.Sink)
			So(resp.EventBus, ShouldEqual, request.EventBus)
			resp2, err := ctrl.CreateSubscription(ctx, create)
			So(err, ShouldBeNil)
			So(resp2.Sink, ShouldEqual, request.Sink)
			So(resp2.EventBus, ShouldEqual, request.EventBus)
			So(resp.Id, ShouldNotEqual, resp2.Id)
		})
	})
}

func TestController_UpdateSubscription(t *testing.T) {
	Convey("test update subscription", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)

		subID := vanus.NewID()
		ctrl.state = primitive.ServerStateRunning
		Convey("update subscription not exist", func() {
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).Return(nil)
			request := &ctrlpb.UpdateSubscriptionRequest{
				Subscription: &ctrlpb.SubscriptionRequest{
					Sink: "test-sink",
				},
				Id: subID.Uint64(),
			}
			_, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldNotBeNil)
		})
		sub := metadata.Subscription{
			ID:            subID,
			Phase:         metadata.SubscriptionPhaseRunning,
			TriggerWorker: "test-addr",
			EventBus:      "test-eb",
			Sink:          "test-sink",
			Protocol:      primitive.HTTPProtocol,
		}
		b, _ := stdJson.Marshal(sub)
		var _sub *metadata.Subscription
		_ = stdJson.Unmarshal(b, &_sub)
		subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).AnyTimes().Return(_sub)
		Convey("no change", func() {
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "test-sink",
				},
			}
			_, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldNotBeNil)
		})
		Convey("tet update eventbus fail", func() {
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb-modify",
					Sink:     "test-sink",
				},
			}
			_, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldNotBeNil)
		})
		Convey("update dead letter fail", func() {
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "test-sink",
					Config: &metapb.SubscriptionConfig{
						DeadLetterEventbus: "new-bus",
					},
				},
			}
			_, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldNotBeNil)
		})
		Convey("test update sink credential", func() {
			Convey("sink is invalid", func() {
				request := &ctrlpb.UpdateSubscriptionRequest{
					Id: subID.Uint64(),
					Subscription: &ctrlpb.SubscriptionRequest{
						EventBus: "test-eb",
						Sink:     "test-sink",
						Protocol: metapb.Protocol_AWS_LAMBDA,
						SinkCredential: &metapb.SinkCredential{
							CredentialType: metapb.SinkCredential_CLOUD,
						},
					},
				}
				_, err := ctrl.UpdateSubscription(ctx, request)
				So(err, ShouldNotBeNil)
			})
			Convey("credential type is invalid", func() {
				request := &ctrlpb.UpdateSubscriptionRequest{
					Id: subID.Uint64(),
					Subscription: &ctrlpb.SubscriptionRequest{
						EventBus: "test-eb",
						Sink:     "arn:aws:lambda:us-west-2:843378899134:function:xdltest",
						Protocol: metapb.Protocol_AWS_LAMBDA,
						SinkCredential: &metapb.SinkCredential{
							CredentialType: metapb.SinkCredential_PLAIN,
						},
					},
				}
				_, err := ctrl.UpdateSubscription(ctx, request)
				So(err, ShouldNotBeNil)
			})
			Convey("ak,sk is empty", func() {
				request := &ctrlpb.UpdateSubscriptionRequest{
					Id: subID.Uint64(),
					Subscription: &ctrlpb.SubscriptionRequest{
						EventBus: "test-eb",
						Sink:     "arn:aws:lambda:us-west-2:843378899134:function:xdltest",
						Protocol: metapb.Protocol_AWS_LAMBDA,
						SinkCredential: &metapb.SinkCredential{
							CredentialType: metapb.SinkCredential_CLOUD,
						},
					},
				}
				_, err := ctrl.UpdateSubscription(ctx, request)
				So(err, ShouldNotBeNil)
			})
			Convey("success", func() {
				request := &ctrlpb.UpdateSubscriptionRequest{
					Id: subID.Uint64(),
					Subscription: &ctrlpb.SubscriptionRequest{
						EventBus: "test-eb",
						Sink:     "arn:aws:lambda:us-west-2:843378899134:function:xdltest",
						Protocol: metapb.Protocol_AWS_LAMBDA,
						SinkCredential: &metapb.SinkCredential{
							CredentialType: metapb.SinkCredential_CLOUD,
							Credential: &metapb.SinkCredential_Cloud{
								Cloud: &metapb.CloudCredential{
									AccessKeyId:     "test_ak",
									SecretAccessKey: "test_sk",
								},
							},
						},
					},
				}
				subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
				resp, err := ctrl.UpdateSubscription(ctx, request)
				So(err, ShouldBeNil)
				So(resp.Protocol, ShouldEqual, request.Subscription.Protocol)
				So(resp.SinkCredential.CredentialType, ShouldEqual, request.Subscription.SinkCredential.CredentialType)
				So(resp.SinkCredential.GetCloud().AccessKeyId, ShouldEqual, primitive.SecretsMask)
				So(resp.SinkCredential.GetCloud().SecretAccessKey, ShouldEqual, primitive.SecretsMask)
			})
		})
		Convey("update rate limit", func() {
			subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "test-sink",
					Config: &metapb.SubscriptionConfig{
						RateLimit: -1,
					},
				},
			}
			resp, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldBeNil)
			So(resp.Config.RateLimit, ShouldEqual, request.Subscription.Config.RateLimit)
		})
		Convey("update subscription sink", func() {
			subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "modify-sink",
				},
			}
			resp, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldBeNil)
			So(resp.Sink, ShouldEqual, request.Subscription.Sink)
			So(resp.Sink, ShouldNotEqual, sub.Sink)
			So(resp.EventBus, ShouldEqual, sub.EventBus)
		})
		Convey("update filters", func() {
			subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "test-sink",
					Filters: []*metapb.Filter{
						{
							Exact: map[string]string{"type": "test"},
						},
					},
				},
			}
			resp, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldBeNil)
			So(len(resp.Filters), ShouldEqual, 1)
			So(len(sub.Filters), ShouldEqual, 0)
		})
		Convey("update transformer", func() {
			subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
			request := &ctrlpb.UpdateSubscriptionRequest{
				Id: subID.Uint64(),
				Subscription: &ctrlpb.SubscriptionRequest{
					EventBus: "test-eb",
					Sink:     "test-sink",
					Transformer: &metapb.Transformer{
						Define:   map[string]string{"k": "v"},
						Template: "test",
					},
				},
			}
			resp, err := ctrl.UpdateSubscription(ctx, request)
			So(err, ShouldBeNil)
			So(resp.Transformer, ShouldNotBeNil)
			So(sub.Transformer, ShouldBeNil)
		})
	})
}

func TestController_DeleteSubscription(t *testing.T) {
	Convey("test delete subscription", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)

		subID := vanus.NewID()
		request := &ctrlpb.DeleteSubscriptionRequest{
			Id: subID.Uint64(),
		}
		ctrl.state = primitive.ServerStateRunning
		Convey("delete subscription no exist", func() {
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).Return(nil)
			_, err := ctrl.DeleteSubscription(ctx, request)
			So(err, ShouldBeNil)
		})
		Convey("delete subscription exist", func() {
			addr := "test"
			tWorker := worker.NewMockTriggerWorker(mockCtrl)
			sub := &metadata.Subscription{
				ID:            subID,
				TriggerWorker: addr,
			}
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).Return(sub)
			subManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).Return(nil)
			workerManager.EXPECT().GetTriggerWorker(addr).Return(tWorker)
			tWorker.EXPECT().UnAssignSubscription(gomock.Eq(subID)).Return()
			Convey("delete subscription success", func() {
				subManager.EXPECT().DeleteSubscription(gomock.Any(), gomock.Eq(subID)).Return(nil)
				_, err := ctrl.DeleteSubscription(ctx, request)
				So(err, ShouldBeNil)
				So(sub.Phase, ShouldEqual, metadata.SubscriptionPhaseToDelete)
				time.Sleep(10 * time.Millisecond)
				ctrl.lock.Lock()
				_, exist := ctrl.needCleanSubscription[subID]
				ctrl.lock.Unlock()
				So(exist, ShouldBeFalse)
			})
			Convey("delete subscription fail", func() {
				subManager.EXPECT().DeleteSubscription(gomock.Any(), gomock.Eq(subID)).Return(fmt.Errorf("error"))
				_, err := ctrl.DeleteSubscription(ctx, request)
				So(err, ShouldBeNil)
				So(sub.Phase, ShouldEqual, metadata.SubscriptionPhaseToDelete)
				time.Sleep(10 * time.Millisecond)
				ctrl.lock.Lock()
				_, exist := ctrl.needCleanSubscription[subID]
				ctrl.lock.Unlock()
				So(exist, ShouldBeTrue)
			})
		})
	})
}

func TestController_GetSubscription(t *testing.T) {
	Convey("test get subscription", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)

		subID := vanus.NewID()
		request := &ctrlpb.GetSubscriptionRequest{
			Id: subID.Uint64(),
		}
		ctrl.state = primitive.ServerStateRunning
		Convey("get subscription no exist", func() {
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).Return(nil)
			_, err := ctrl.GetSubscription(ctx, request)
			So(err, ShouldNotBeNil)
		})
		Convey("get subscription exist", func() {
			sub := &metadata.Subscription{
				ID:       subID,
				EventBus: "test-bus",
			}
			subManager.EXPECT().GetSubscription(gomock.Any(), gomock.Eq(subID)).Return(sub)
			subManager.EXPECT().GetOffset(gomock.Any(), gomock.Any()).Return(info.ListOffsetInfo{}, nil)
			resp, err := ctrl.GetSubscription(ctx, request)
			So(err, ShouldBeNil)
			So(resp.EventBus, ShouldEqual, sub.EventBus)
			So(resp.Id, ShouldEqual, sub.ID)
		})
	})
}

func TestController_ListSubscription(t *testing.T) {
	Convey("test list subscription", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)
		Convey("list subscription", func() {
			list := []*metadata.Subscription{
				{ID: vanus.NewID(), EventBus: "bus1"},
				{ID: vanus.NewID(), EventBus: "bus2"},
			}
			subManager.EXPECT().ListSubscription(gomock.Any()).Return(list)
			subManager.EXPECT().GetOffset(gomock.Any(), gomock.Any()).AnyTimes().Return(info.ListOffsetInfo{}, nil)
			resp, err := ctrl.ListSubscription(ctx, nil)
			So(err, ShouldBeNil)
			So(len(resp.Subscription), ShouldEqual, 2)
		})
	})
}

func TestController_TriggerWorkerHeartbeat(t *testing.T) {
	Convey("test trigger worker heartbeat", t, func() {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		ctrl := NewController(Config{}, nil)
		ctx := context.Background()
		workerManager := worker.NewMockManager(mockCtrl)
		ctrl.workerManager = workerManager
		subManager := subscription.NewMockManager(mockCtrl)
		ctrl.subscriptionManager = subManager
		ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)
		subID1 := vanus.NewID()
		sub1 := &metapb.SubscriptionInfo{
			SubscriptionId: subID1.Uint64(),
			Offsets: []*metapb.OffsetInfo{
				{
					EventLogId: vanus.NewID().Uint64(),
					Offset:     100,
				},
			},
		}
		subID2 := vanus.NewID()
		sub2 := &metapb.SubscriptionInfo{
			SubscriptionId: subID2.Uint64(),
			Offsets: []*metapb.OffsetInfo{
				{
					EventLogId: vanus.NewID().Uint64(),
					Offset:     100,
				},
			},
		}
		subID3 := vanus.NewID()
		sub3 := &metapb.SubscriptionInfo{
			SubscriptionId: subID3.Uint64(),
			Offsets:        []*metapb.OffsetInfo{},
		}
		request := &ctrlpb.TriggerWorkerHeartbeatRequest{
			Address:          "test",
			SubscriptionInfo: []*metapb.SubscriptionInfo{sub1, sub2, sub3},
		}
		subManager.EXPECT().Heartbeat(gomock.Any(), gomock.Eq(subID1), request.Address, gomock.Any()).AnyTimes().Return(fmt.Errorf("error"))
		subManager.EXPECT().Heartbeat(gomock.Any(), gomock.Eq(subID2), request.Address, gomock.Any()).AnyTimes().Return(nil)
		subManager.EXPECT().Heartbeat(gomock.Any(), gomock.Eq(subID3), request.Address, gomock.Any()).AnyTimes().Return(nil)
		Convey("heartbeat error", func() {
			workerManager.EXPECT().UpdateTriggerWorkerInfo(gomock.Any(), gomock.Eq(request.Address)).Return(fmt.Errorf("error"))
			err := ctrl.triggerWorkerHeartbeatRequest(ctx, request)
			So(err, ShouldNotBeNil)
		})
		Convey("heartbeat success", func() {
			workerManager.EXPECT().UpdateTriggerWorkerInfo(gomock.Any(), gomock.Eq(request.Address)).Return(nil)
			subManager.EXPECT().SaveOffset(gomock.Any(), gomock.Eq(subID1), gomock.Any(), false).Return(nil)
			subManager.EXPECT().SaveOffset(gomock.Any(), gomock.Eq(subID2), gomock.Any(), false).Return(fmt.Errorf("error"))
			err := ctrl.triggerWorkerHeartbeatRequest(ctx, request)
			So(err, ShouldBeNil)
		})
	})
}
