package cluster

import (
	"context"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
)

var (
	systemEventbusPrefix          = "__"
	defaultSystemEventbusEventlog = 1
)

type eventbusService struct {
	client ctrlpb.EventbusControllerClient
	nsSvc  NamespaceService
}

func newEventbusService(cc *raw_client.Conn, svc NamespaceService) EventbusService {
	return &eventbusService{
		client: raw_client.NewEventbusClient(cc),
		nsSvc:  svc,
	}
}

func (es *eventbusService) GetSystemEventbusByName(ctx context.Context, name string) (*meta.Eventbus, error) {
	pb, err := es.nsSvc.GetSystemNamespace(ctx)
	if err != nil {
		return nil, err
	}
	return es.client.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
		NamespaceId:  pb.Id,
		EventbusName: name,
	})
}

func (es *eventbusService) GetEventbus(ctx context.Context, id uint64) (*meta.Eventbus, error) {
	return es.client.GetEventbus(ctx, wrapperspb.UInt64(id))
}

func (es *eventbusService) IsSystemEventbusExistByName(ctx context.Context, name string) (bool, error) {
	nsPb, err := es.nsSvc.GetSystemNamespace(ctx)
	if err != nil {
		return false, err
	}
	ebPb, err := es.client.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
		NamespaceId:  nsPb.Id,
		EventbusName: name,
	})
	return !(ebPb == nil), err
}

func (es *eventbusService) IsExist(ctx context.Context, id uint64) bool {
	_, err := es.client.GetEventbus(ctx, wrapperspb.UInt64(id))
	return err == nil
}

func (es *eventbusService) CreateSystemEventbusIfNotExist(ctx context.Context, name string, desc string) (*meta.Eventbus, error) {
	exist, err := es.IsSystemEventbusExistByName(ctx, name)
	if err != nil && !errors.Is(err, errors.ErrResourceNotFound) {
		return nil, err
	}

	if exist {
		return nil, nil
	}

	nsPb, err := es.nsSvc.GetSystemNamespace(ctx)
	if err != nil {
		return nil, err
	}

	return es.client.CreateSystemEventbus(ctx, &ctrlpb.CreateEventbusRequest{
		Name:        name,
		LogNumber:   int32(defaultSystemEventbusEventlog),
		Description: desc,
		NamespaceId: nsPb.Id,
	})
}

func (es *eventbusService) Delete(ctx context.Context, id uint64) error {
	_, err := es.client.DeleteEventbus(ctx, wrapperspb.UInt64(id))
	return err
}

func (es *eventbusService) RawClient() ctrlpb.EventbusControllerClient {
	return es.client
}
