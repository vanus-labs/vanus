package cluster

import (
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
)

type idService struct {
	client ctrlpb.SnowflakeControllerClient
}

func newIDService(cc *raw_client.Conn) IDService {
	return &idService{client: raw_client.NewSnowflakeController(cc)}
}

func (es *idService) RawClient() ctrlpb.SnowflakeControllerClient {
	return es.client
}
