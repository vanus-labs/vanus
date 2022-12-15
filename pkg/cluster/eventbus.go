package cluster

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"strings"
)

var (
	systemEventbusPrefix = "__"
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

func (es *eventbusService) CreateSystemEventbusIfNotExist(ctx context.Context, name string, logNum int, desc string) error {
	if es.IsExist(ctx, name) {
		return nil
	}

	// TODO 创建前需要等到Store就绪，而store的就绪在controller之后，创建又在controller就绪过程中
	_, err := es.client.CreateSystemEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name:        name,
		LogNumber:   int32(logNum),
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
