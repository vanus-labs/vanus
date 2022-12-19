package cluster

import (
	"github.com/linkall-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
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
