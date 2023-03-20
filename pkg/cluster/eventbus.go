package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	systemEventbusPrefix          = "__"
	defaultSystemEventbusEventlog = 1
)

type eventbusService struct {
	client ctrlpb.EventbusControllerClient
	nsSvc  NamespaceService
	cache  sync.Map
}

func newEventbusService(cc *raw_client.Conn, svc NamespaceService) EventbusService {
	return &eventbusService{
		client: raw_client.NewEventbusClient(cc),
		nsSvc:  svc,
	}
}

func (es *eventbusService) GetSystemEventbusByName(ctx context.Context, name string) (*meta.Eventbus, error) {
	return es.GetEventbusByName(ctx, systemNamespace, name)
}

func (es *eventbusService) GetEventbusByName(ctx context.Context, ns, name string) (*meta.Eventbus, error) {
	key := fmt.Sprintf("%s_%s", ns, name)
	v, exist := es.cache.Load(key)
	if exist {
		return v.(*meta.Eventbus), nil
	}

	pb, err := es.nsSvc.GetNamespaceByName(ctx, ns)
	if err != nil {
		return nil, err
	}
	eb, err := es.client.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
		NamespaceId:  pb.Id,
		EventbusName: name,
	})
	if err != nil {
		return nil, err
	}

	es.cache.Store(key, eb)
	return eb, nil
}

func (es *eventbusService) GetEventbus(ctx context.Context, id uint64) (*meta.Eventbus, error) {
	v, exist := es.cache.Load(id)
	if exist {
		return v.(*meta.Eventbus), nil
	}

	eb, err := es.client.GetEventbus(ctx, wrapperspb.UInt64(id))
	if err != nil {
		return nil, err
	}

	es.cache.Store(id, eb)
	return eb, nil
}

func (es *eventbusService) IsSystemEventbusExistByName(ctx context.Context, name string) (bool, error) {
	ebPb, err := es.GetSystemEventbusByName(ctx, name)
	return ebPb != nil, err
}

func (es *eventbusService) CreateSystemEventbusIfNotExist(ctx context.Context, name string, desc string) (*meta.Eventbus, error) {
	if !strings.HasPrefix(name, systemEventbusPrefix) {
		return nil, errors.New("invalid system eventbus name")
	}
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
