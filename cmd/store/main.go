// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/store/segment"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	"google.golang.org/grpc"
	"net"
	"os"
	"time"
)

var (
	defaultIP   = "127.0.0.1"
	defaultPort = 11811

	defaultControllerAddr = "127.0.0.1:2048"
)

func main() {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", defaultIP, defaultPort))
	if err != nil {
		log.Error(context.Background(), "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}
	var opts []grpc.ServerOption
	//opts = append(opts, grpc_error.GRPCErrorServerOutboundInterceptor()...)
	grpcServer := grpc.NewServer(opts...)
	exitChan := make(chan struct{}, 1)
	stopCallback := func() {
		grpcServer.GracefulStop()
		exitChan <- struct{}{}
	}
	srv := segment.NewSegmentServer("127.0.0.1:11811", defaultControllerAddr,
		"volume-1", stopCallback)
	if err != nil {
		stopCallback()
		log.Error(context.Background(), "start SegmentServer failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	ctx := context.Background()
	init, _ := srv.(primitive.Initializer)
	// TODO panic
	if err = init.Initialize(ctx); err != nil {
		stopCallback()
		log.Error(ctx, "the SegmentServer has initialized failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}
	go func() {
		log.Info(ctx, "the SegmentServer ready to work", map[string]interface{}{
			"listen_ip":   defaultIP,
			"listen_port": defaultPort,
			"time":        util.FormatTime(time.Now()),
		})
		segpb.RegisterSegmentServerServer(grpcServer, srv)
		if err = grpcServer.Serve(listen); err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}()

	<-exitChan
	log.Info(ctx, "the grpc server has been shutdown", nil)
}
