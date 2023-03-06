package cluster

import (
	"context"
	"fmt"
	"strings"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
)

var (
	systemEventbusPrefix          = "__"
	defaultSystemEventbusEventlog = 1
)

type eventbusService struct {
	client ctrlpb.EventBusControllerClient
}

func newEventbusService(cc *raw_client.Conn) EventbusService {
	return &eventbusService{client: raw_client.NewEventbusClient(cc)}
}

func (es *eventbusService) IsExist(ctx context.Context, name string) bool {
	_, err := es.client.GetEventBus(ctx, &metapb.EventBus{
		Name: name,
	})
	return err == nil
}

func (es *eventbusService) CreateSystemEventbusIfNotExist(ctx context.Context, name string, desc string) error {
	if es.IsExist(ctx, name) {
		return nil
	}

	_, err := es.client.CreateSystemEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name:        name,
		LogNumber:   int32(defaultSystemEventbusEventlog),
		Description: desc,
	})
	return err
}

func (es *eventbusService) Delete(ctx context.Context, name string) error {
	if !strings.HasPrefix(name, systemEventbusPrefix) {
		return fmt.Errorf("the system eventbus must start with %s", systemEventbusPrefix)
	}

	_, err := es.client.DeleteEventBus(ctx, &metapb.EventBus{
		Name: name,
	})
	return err
}

func (es *eventbusService) RawClient() ctrlpb.EventBusControllerClient {
	return es.client
}
