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
	// standard libraries.
	"context"
	"flag"
	"fmt"
	"net"
	"os"

	// third-party libraries.
	"google.golang.org/grpc"

	// first-party libraries.
	raftpb "github.com/linkall-labs/vanus/proto/pkg/raft"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/segment"
	"github.com/linkall-labs/vanus/observability/log"
)

var (
	f = flag.String("config", "./config/store.yaml", "store config file path")
)

func main() {
	flag.Parse()

	cfg, err := store.InitConfig(*f)
	if err != nil {
		log.Error(nil, "init config error", map[string]interface{}{log.KeyError: err})
		os.Exit(-1)
	}

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
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

	srv, raftSrv := segment.NewSegmentServer(*cfg, stopCallback)
	if err != nil {
		stopCallback()
		log.Error(context.Background(), "start SegmentServer failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	segpb.RegisterSegmentServerServer(grpcServer, srv)
	raftpb.RegisterRaftServerServer(grpcServer, raftSrv)

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
			"listen_ip":   cfg.IP,
			"listen_port": cfg.Port,
		})
		if err = grpcServer.Serve(listen); err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
			return
		}
	}()

	<-exitChan
	log.Info(ctx, "the grpc server has been shutdown", nil)
}
