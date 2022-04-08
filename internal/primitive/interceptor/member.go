package interceptor

import (
	"context"
	"fmt"
	embedetcd "github.com/linkall-labs/embed-etcd"
	rpcerr "github.com/linkall-labs/vsproto/pkg/errors"
	"google.golang.org/grpc"
)

func CheckLeadershipInterceptor(member embedetcd.Member, svcMapping map[string]string) []grpc.ServerOption {
	opts := make([]grpc.ServerOption, 0)
	opts = append(opts, grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		if !member.IsLeader() {
			// TODO  read-only request bypass
			return nil, rpcerr.New(fmt.Sprintf("i'm not leader, please connect to: %s",
				svcMapping[member.GetLeaderID()])).WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
		}
		res, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return res, err
	}))
	// TODO add stream interceptor
	return opts
}
