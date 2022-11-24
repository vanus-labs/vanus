package proxy

import (
	stdCtx "context"
	"testing"

	"github.com/golang/mock/gomock"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestControllerProxy_ProxyMethod(t *testing.T) {
	Convey("test get event", t, func() {
		cp := NewControllerProxy(Config{
			Endpoints: []string{"127.0.0.1:20001",
				"127.0.0.1:20002", "127.0.0.1:20003"},
			ProxyPort:              18082,
			CloudEventReceiverPort: 18080,
			Credentials:            insecure.NewCredentials(),
		})
		ctrl := gomock.NewController(t)

		eventbusCtrl := ctrlpb.NewMockEventBusControllerClient(ctrl)
		cp.eventbusCtrl = eventbusCtrl
		eventbusCtrl.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().DeleteEventBus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().GetEventBus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		eventbusCtrl.EXPECT().ListEventBus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		_, _ = cp.CreateEventBus(stdCtx.Background(), &ctrlpb.CreateEventBusRequest{})
		_, _ = cp.DeleteEventBus(stdCtx.Background(), &metapb.EventBus{})
		_, _ = cp.GetEventBus(stdCtx.Background(), &metapb.EventBus{})
		_, _ = cp.ListEventBus(stdCtx.Background(), &emptypb.Empty{})
		_, err := cp.UpdateEventBus(stdCtx.Background(), &ctrlpb.UpdateEventBusRequest{})
		So(err, ShouldEqual, errMethodNotImplemented)

		eventlogCtrl := ctrlpb.NewMockEventLogControllerClient(ctrl)
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
		_, _ = cp.ListSubscription(stdCtx.Background(), &emptypb.Empty{})
	})
}
