package cluster

import (
	"context"
	"time"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	"github.com/vanus-labs/vanus/pkg/errors"
)

type triggerService struct {
	client ctrlpb.TriggerControllerClient
}

func newTriggerService(cc *raw_client.Conn) TriggerService {
	return &triggerService{client: raw_client.NewTriggerClient(cc)}
}

func (es *triggerService) RawClient() ctrlpb.TriggerControllerClient {
	return es.client
}

func (es *triggerService) RegisterHeartbeat(ctx context.Context, interval time.Duration, reqFunc func() interface{}) error {
	return raw_client.RegisterHeartbeat(ctx, interval, es.client, reqFunc)
}

func (es *triggerService) GetSubscription(ctx context.Context, id uint64) (*metapb.Subscription, error) {
	subscription, err := es.client.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: id})
	return subscription, errors.To(err)
}
