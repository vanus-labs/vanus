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
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"google.golang.org/grpc"
	"net"
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
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	exitChan := make(chan struct{}, 1)
	//stopCallback := func() {
	//	grpcServer.GracefulStop()
	//	exitChan <- struct{}{}
	//}
	controller.RegisterEventBusControllerServer(grpcServer, eventbus.NewEventBusController())
	controller.RegisterEventLogControllerServer(grpcServer, eventbus.NewEventLogController())
	controller.RegisterSegmentControllerServer(grpcServer, eventbus.NewSegmentController())
	log.Info("the grpc server ready to work", nil)
	err = grpcServer.Serve(listen)
	if err != nil {
		log.Error("grpc server occurred an error", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		<-exitChan
		log.Info("the grpc server has been shutdown", nil)
	}
}
