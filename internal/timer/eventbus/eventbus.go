package eventbus

import (
	"context"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/linkall-labs/vanus/observability/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

var (
	c *Config
)

func Init(cfg *Config) {
	c = cfg
}

// CreateEventBus
func CreateEventBus(ctx context.Context, eventbus string) error {
	grpcConn := mustGetControllerProxyConn(ctx, c.Endpoints)
	defer func() {
		_ = grpcConn.Close()
	}()

	cli := ctrlpb.NewEventBusControllerClient(grpcConn)
	res, err := cli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name: eventbus,
	})
	if err != nil {
		log.Error(ctx, "create eventbus failed", map[string]interface{}{
			log.KeyError: err,
			"res":        res,
		})
		return nil
	}
	// log.Info(ctx, "create eventbus success.", map[string]interface{}{
	// 	"res": res,
	// })
	return nil
}

// DeleteEventBus todo
// func (c *Config) DeleteEventBus(ctx context.Context) error {
// 	return nil
// }

func mustGetControllerProxyConn(ctx context.Context, endpoints []string) *grpc.ClientConn {
	var leaderConn *grpc.ClientConn
	for _, endpoint := range endpoints {
		leaderConn = createGRPCConn(ctx, endpoint)
		if leaderConn == nil {
			continue
		}
		pingClient := ctrlpb.NewPingServerClient(leaderConn)
		res, err := pingClient.Ping(context.Background(), &emptypb.Empty{})
		if err != nil {
			log.Warning(ctx, "ping failed", map[string]interface{}{
				log.KeyError: err,
			})
			continue
		}
		log.Info(ctx, "get leader address success", map[string]interface{}{
			"LeaderAddr": res.LeaderAddr,
		})
		if endpoint == res.LeaderAddr {
			break
		}
		leaderConn = createGRPCConn(ctx, res.LeaderAddr)
		break
	}
	return leaderConn
}

func createGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	addr = strings.TrimPrefix(addr, "http://")
	var err error
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithCancel(ctx)
	timeout := false
	go func() {
		ticker := time.Tick(time.Second)
		select {
		case <-ctx.Done():
		case <-ticker:
			cancel()
			timeout = true
		}
	}()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	cancel()
	if timeout {
		color.Yellow("dial to controller: %s timeout, try to another controller", addr)
		return nil
	} else if err != nil {
		color.Red("dial to controller: %s failed", addr)
		return nil
	}
	return conn
}
