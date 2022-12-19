package cluster

import (
	"github.com/linkall-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

type eventlogService struct {
	client ctrlpb.EventLogControllerClient
}

func newEventlogService(cc *raw_client.Conn) EventlogService {
	return &eventlogService{client: raw_client.NewEventlogClient(cc)}
}

func (es *eventlogService) RawClient() ctrlpb.EventLogControllerClient {
	return es.client
}
