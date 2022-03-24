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
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"google.golang.org/grpc"
	"net"
	"os"
)

var (
	defaultIP   = "127.0.0.1"
	defaultPort = 2048
)

func main() {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", defaultIP, defaultPort))
	if err != nil {
		log.Info(context.Background(), "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(0)
	}
	var opts []grpc.ServerOption
	opts = append(opts, interceptor.GRPCErrorServerOutboundInterceptor()...)
	ctrlSrv := eventbus.NewEventBusController(eventbus.ControllerConfig{
		IP:               defaultIP,
		Port:             defaultPort,
		KVStoreEndpoints: []string{"127.0.0.1:2379"},
		KVKeyPrefix:      "/wenfeng",
	})
	ctx := context.Background()
	if err = ctrlSrv.Start(ctx); err != nil {
		log.Error(ctx, "start Eventbus Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	grpcServer := grpc.NewServer(opts...)
	exitChan := make(chan struct{}, 0)
	controller.RegisterEventBusControllerServer(grpcServer, ctrlSrv)
	controller.RegisterEventLogControllerServer(grpcServer, ctrlSrv)
	controller.RegisterSegmentControllerServer(grpcServer, ctrlSrv)
	log.Info(ctx, "the grpc server ready to work", nil)
	err = grpcServer.Serve(listen)
	if err != nil {
		log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		<-exitChan
		log.Info(ctx, "the grpc server has been shutdown", nil)
	}
}
