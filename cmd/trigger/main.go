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
	"github.com/linkall-labs/vanus/pkg/util/signal"
	"net"
	"net/http"
	"os"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/credential"
	"github.com/linkall-labs/vanus/internal/trigger"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	configPath = flag.String("config", "./config/trigger.yaml", "trigger worker config file path")
)

func main() {
	flag.Parse()

	cfg, err := trigger.InitConfig(*configPath)
	if err != nil {
		log.Error(context.Background(), "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	ctx := signal.SetupSignalContext()
	err = startMetrics(cfg.TLS)
	if err != nil {
		log.Error(context.Background(), "start metric server fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	srv := trigger.NewTriggerServer(*cfg)
	grpcServer, err := startGrpcServer(ctx, cfg.TLS, cfg.Port, srv)
	if err != nil {
		log.Error(context.Background(), "start grpc server fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	init := srv.(primitive.Initializer)
	if err = init.Initialize(ctx); err != nil {
		log.Error(ctx, "the trigger worker has initialized failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	<-ctx.Done()
	closer := srv.(primitive.Closer)
	closer.Close(ctx)
	grpcServer.GracefulStop()
	log.Info(ctx, "trigger worker stopped", nil)
}

func startMetrics(tlsInfo credential.TLSInfo) error {
	metrics.RegisterTriggerMetrics()
	listen, err := net.Listen("tcp", fmt.Sprintf(":2112"))
	if err != nil {
		return err
	}
	metricServer := &http.Server{}
	if !tlsInfo.Empty() {
		tlsCfg, err := tlsInfo.ServerConfig()
		if err != nil {
			return err
		}
		metricServer.TLSConfig = tlsCfg
	}
	http.Handle("/metrics", promhttp.Handler())
	go metricServer.Serve(listen)
	return nil
}

func startGrpcServer(ctx context.Context, tlsInfo credential.TLSInfo, port int, srv pbtrigger.TriggerWorkerServer) (*grpc.Server, error) {
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	var opts []grpc.ServerOption
	if !tlsInfo.Empty() {
		tlsCfg, err := tlsInfo.ServerConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	grpcServer := grpc.NewServer(opts...)
	pbtrigger.RegisterTriggerWorkerServer(grpcServer, srv)
	go func() {
		log.Info(ctx, "the grpc server ready to work", nil)
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}()
	return grpcServer, nil
}
