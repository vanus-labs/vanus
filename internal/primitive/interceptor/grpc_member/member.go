package grpc_member

import (
	"context"
	"fmt"
	embedetcd "github.com/linkall-labs/embed-etcd"
	rpcerr "github.com/linkall-labs/vsproto/pkg/errors"
	"google.golang.org/grpc"
)

func StreamServerInterceptor(member embedetcd.Member, svcMapping map[string]string) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !member.IsLeader() {
			// TODO  read-only request bypass
			return rpcerr.New(fmt.Sprintf("i'm not leader, please connect to: %s",
				svcMapping[member.GetLeaderID()])).WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
		}
		return handler(srv, stream)
	}
}

func UnaryServerInterceptor(member embedetcd.Member,
	svcMapping map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		if !member.IsLeader() {
			// TODO  read-only request bypass
			return nil, rpcerr.New(fmt.Sprintf("i'm not leader, please connect to: %s",
				svcMapping[member.GetLeaderID()])).WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
		}
		return handler(ctx, req)
	}
}
