package proxy

import (
	stdCtx "context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/pkg/authorization"
)

func TestControllerProxy_ProxyMethod(t *testing.T) {
	Convey("test get event", t, func() {
		cp := NewControllerProxy(Config{
			Endpoints: []string{
				"127.0.0.1:20001",
				"127.0.0.1:20002", "127.0.0.1:20003",
			},
			ProxyPort:              18082,
			CloudEventReceiverPort: 18080,
			Credentials:            insecure.NewCredentials(),
		})
		ctrl := gomock.NewController(t)

		eventbusCtrl := ctrlpb.NewMockEventbusControllerClient(ctrl)
		cp.eventbusCtrl = eventbusCtrl
		roleClient := authorization.NewMockRoleClient(ctrl)
		cp.authService.RoleClient = roleClient
		roleClient.EXPECT().IsClusterAdmin(gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
		eventbusCtrl.EXPECT().CreateEventbus(gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().DeleteEventbus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().GetEventbus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().ListEventbus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		_, _ = cp.CreateEventbus(stdCtx.Background(), &ctrlpb.CreateEventbusRequest{})
		_, _ = cp.DeleteEventbus(stdCtx.Background(), &wrapperspb.UInt64Value{})
		_, _ = cp.GetEventbus(stdCtx.Background(), &wrapperspb.UInt64Value{})
		_, _ = cp.ListEventbus(stdCtx.Background(), &ctrlpb.ListEventbusRequest{})
		_, err := cp.UpdateEventbus(stdCtx.Background(), &ctrlpb.UpdateEventbusRequest{})
		So(err, ShouldEqual, errMethodNotImplemented)

		eventlogCtrl := ctrlpb.NewMockEventlogControllerClient(ctrl)
		cp.eventlogCtrl = eventlogCtrl
		eventlogCtrl.EXPECT().ListSegment(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		_, _ = cp.ListSegment(stdCtx.Background(), &ctrlpb.ListSegmentRequest{})

		triggerCtrl := ctrlpb.NewMockTriggerControllerClient(ctrl)
		cp.triggerCtrl = triggerCtrl
		triggerCtrl.EXPECT().CreateSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		triggerCtrl.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		triggerCtrl.EXPECT().DeleteSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		triggerCtrl.EXPECT().GetSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		triggerCtrl.EXPECT().ListSubscription(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		_, _ = cp.CreateSubscription(stdCtx.Background(), &ctrlpb.CreateSubscriptionRequest{})
		_, _ = cp.UpdateSubscription(stdCtx.Background(), &ctrlpb.UpdateSubscriptionRequest{})
		_, _ = cp.DeleteSubscription(stdCtx.Background(), &ctrlpb.DeleteSubscriptionRequest{})
		_, _ = cp.GetSubscription(stdCtx.Background(), &ctrlpb.GetSubscriptionRequest{})
		_, _ = cp.ListSubscription(stdCtx.Background(), &ctrlpb.ListSubscriptionRequest{})
	})
}
