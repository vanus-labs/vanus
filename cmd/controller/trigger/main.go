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
	"flag"
	"fmt"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/internal/util/signal"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"google.golang.org/grpc"
	"net"
	"os"
	"time"
)

func main() {
	ctx := signal.SetupSignalContext()
	f := flag.String("conf", "./config/trigger.yaml", "trigger controller config file path")
	flag.Parse()
	c, err := trigger.Init(*f)
	if err != nil {
		log.Error(ctx, "init config error", map[string]interface{}{log.KeyError: err})
		os.Exit(-1)
	}
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", c.Port))
	if err != nil {
		log.Error(ctx, "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}
	srv := trigger.NewTriggerController(*c)
	if err = srv.Start(); err != nil {
		log.Error(context.Background(), "trigger controller start fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	controller.RegisterTriggerControllerServer(grpcServer, srv)
	log.Info(ctx, "the TriggerControlServer ready to work", map[string]interface{}{
		"listen_port": c.Port,
		"time":        util.FormatTime(time.Now()),
	})
	go func() {
		<-ctx.Done()
		log.Info(ctx, "triggerController begin stop", nil)
		grpcServer.GracefulStop()
		srv.Close()
	}()

	if err = grpcServer.Serve(listen); err != nil {
		log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(1)
	}
	log.Info(ctx, "trigger controller stopped", nil)
}
