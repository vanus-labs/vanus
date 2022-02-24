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
	defaultIP   = "0.0.0.0"
	defaultPort = 11811

	defaultControllerAddr = "127.0.0.1:2048"
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
	stopCallback := func() {
		grpcServer.GracefulStop()
		exitChan <- struct{}{}
	}
	srv := segment.NewSegmentServer("127.0.0.1:11811", defaultControllerAddr,
		"volume-1", stopCallback)
	if err != nil {
		stopCallback()
		log.Error("start SegmentServer failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	go func() {
		log.Info("the SegmentServer ready to work", map[string]interface{}{
			"listen_ip":   defaultIP,
			"listen_port": defaultPort,
			"time":        util.FormatTime(time.Now()),
		})
		segpb.RegisterSegmentServerServer(grpcServer, srv)
		if err = grpcServer.Serve(listen); err != nil {
			log.Error("grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}()
	init, _ := srv.(primitive.Initializer)
	if err = init.Initialize(); err != nil {
		stopCallback()
		log.Error("the SegmentServer has initialized failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}
	<-exitChan
	log.Info("the grpc server has been shutdown", nil)
}
