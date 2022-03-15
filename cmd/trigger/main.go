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
	"github.com/linkall-labs/vanus/config"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/worker"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	defaultIP        = "127.0.0.1"
	defaultPort      = 2149
	trControllerIP   = "127.0.0.1"
	trControllerPort = 2049
)

func main() {
	trControllerAddr := fmt.Sprintf("%s:%d", trControllerIP, trControllerPort)
	trWorkerAddr := fmt.Sprintf("%s:%d", defaultIP, defaultPort)
	listen, err := net.Listen("tcp", trWorkerAddr)
	if err != nil {
		log.Fatal("failed to listen", map[string]interface{}{
			"error": err,
		})
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	stopCallback := func() {
		grpcServer.GracefulStop()
	}
	srv := worker.NewTriggerServer(trControllerAddr, trWorkerAddr, worker.Config{Storage: config.KvStorageConfig{
		ServerList: []string{"127.0.0.1:2379"},
		KeyPrefix:  "/xdl/trigger",
	}}, stopCallback)
	ctx := context.Background()
	init := srv.(primitive.Initializer)
	if err = init.Initialize(ctx); err != nil {
		stopCallback()
		log.Fatal("the trigger worker has initialized failed", map[string]interface{}{
			log.KeyError: err,
		})
	}
	trigger.RegisterTriggerWorkerServer(grpcServer, srv)
	go func() {
		log.Info("the grpc server ready to work", nil)
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error("grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}()

	log.Info("trigger worker started", nil)
	exitCh := make(chan os.Signal)
	signal.Notify(exitCh, os.Interrupt, syscall.SIGTERM)
	<-exitCh
	c := srv.(primitive.Closer)
	if err := c.Close(); err != nil {
		log.Error("the trigger worker close failed", map[string]interface{}{
			log.KeyError: err,
		})
	}
	log.Info("the grpc server has been shutdown", nil)
}
