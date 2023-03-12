package cluster

import (
	"context"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
)

var (
	systemEventbusPrefix          = "__"
	defaultSystemEventbusEventlog = 1
)

type eventbusService struct {
	client            ctrlpb.EventbusControllerClient
	systemNamespaceID uint64
}

func (es *eventbusService) GetSystemEventbusByName(ctx context.Context, name string) (*meta.Eventbus, error) {
	return es.client.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
		NamespaceId:  es.systemNamespaceID,
		EventbusName: name,
	})
}

func (es *eventbusService) GetEventbus(ctx context.Context, id uint64) (*meta.Eventbus, error) {
	return es.client.GetEventbus(ctx, wrapperspb.UInt64(id))
}

func (es *eventbusService) IsSystemEventbusExistByName(ctx context.Context, name string) bool {
	_, err := es.client.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
		NamespaceId:  es.systemNamespaceID,
		EventbusName: name,
	})
	return err == nil
}

func newEventbusService(cc *raw_client.Conn) EventbusService {
	return &eventbusService{client: raw_client.NewEventbusClient(cc)}
}

func (es *eventbusService) IsExist(ctx context.Context, id uint64) bool {
	_, err := es.client.GetEventbus(ctx, wrapperspb.UInt64(id))
	return err == nil
}

func (es *eventbusService) CreateSystemEventbusIfNotExist(ctx context.Context, name string, desc string) (*meta.Eventbus, error) {
	if es.IsSystemEventbusExistByName(ctx, name) {
		return nil, nil
	}

	return es.client.CreateSystemEventbus(ctx, &ctrlpb.CreateEventbusRequest{
		Name:        name,
		LogNumber:   int32(defaultSystemEventbusEventlog),
		Description: desc,
		NamespaceId: es.systemNamespaceID,
	})
}

func (es *eventbusService) Delete(ctx context.Context, id uint64) error {
	_, err := es.client.DeleteEventbus(ctx, wrapperspb.UInt64(id))
	return err
}

func (es *eventbusService) RawClient() ctrlpb.EventbusControllerClient {
	return es.client
}
