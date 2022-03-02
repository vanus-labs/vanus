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
	"fmt"
	"github.com/linkall-labs/vanus/config"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	defaultIP   = "0.0.0.0"
	defaultPort = 2048
)

func main() {
	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", defaultIP, defaultPort))
	if err != nil {
		log.Fatal("failed to listen", map[string]interface{}{
			"error": err,
		})
	}
	srv := trigger.NewTriggerController(trigger.Config{
		Storage: config.KvStorageConfig{
			ServerList: []string{"127.0.0.1:2379"},
			KeyPrefix:  "/xdl/trigger",
		},
	})
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	controller.RegisterTriggerControllerServer(grpcServer, srv)
	go func() {
		log.Info("the TriggerControlServer ready to work", map[string]interface{}{
			"listen_ip":   defaultIP,
			"listen_port": defaultPort,
			"time":        util.FormatTime(time.Now()),
		})
		if err = grpcServer.Serve(listen); err != nil {
			log.Error("grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}()
	if err = srv.Start(); err != nil {
		log.Fatal("trigger controler start fail", map[string]interface{}{
			log.KeyError: err,
		})
	}
	log.Info("triggerController started", nil)
	exitCh := make(chan os.Signal)
	signal.Notify(exitCh, os.Interrupt, syscall.SIGTERM)
	<-exitCh
	log.Info("triggerController begin stop", nil)
	grpcServer.GracefulStop()
	srv.Close()
	log.Info("triggerController stopped", nil)
}
