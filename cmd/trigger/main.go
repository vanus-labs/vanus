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
	"flag"
	"fmt"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"

	"github.com/vanus-labs/vanus/client/pkg/exporter"
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"
	pbtrigger "github.com/vanus-labs/vanus/proto/pkg/trigger"

	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/trigger"
)

var configPath = flag.String("config", "./config/trigger.yaml", "trigger worker config file path")

func main() {
	flag.Parse()

	cfg, err := trigger.InitConfig(*configPath)
	if err != nil {
		log.Error().Err(err).Msg("init config error")
		os.Exit(-1)
	}
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Msg("failed to listen")
		os.Exit(-1)
	}
	ctx := signal.SetupSignalContext()
	cfg.Observability.T.ServerName = "Vanus Trigger"
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetTriggerMetrics, exporter.GetExporter)
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	srv := trigger.NewTriggerServer(*cfg)
	pbtrigger.RegisterTriggerWorkerServer(grpcServer, srv)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx).Msg("the grpc server ready to work")
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx).Err(err).Msg("grpc server occurred an error")
		}
	}()
	init := srv.(primitive.Initializer)
	if err = init.Initialize(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("the trigger worker has initialized failed")
		os.Exit(1)
	}
	<-ctx.Done()
	closer := srv.(primitive.Closer)
	_ = closer.Close(ctx)
	grpcServer.GracefulStop()
	wg.Wait()
	log.Info(ctx).Msg("trigger worker stopped")
}
