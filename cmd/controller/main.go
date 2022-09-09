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
	"sync"

	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller"
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/memberinterceptor"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"

	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	configPath = flag.String("config", "./config/controller.yaml", "the configuration file of controller")
)

func main() {
	flag.Parse()

	cfg, err := controller.InitConfig(*configPath)
	if err != nil {
		log.Error(context.Background(), "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error(context.Background(), "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()
	go startMetrics()
	etcd := embedetcd.New(cfg.Topology)
	if err = etcd.Init(ctx, cfg.GetEtcdConfig()); err != nil {
		log.Error(ctx, "failed to init etcd", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// TODO wait server ready
	segmentCtrl := eventbus.NewController(cfg.GetEventbusCtrlConfig(), etcd)
	if err = segmentCtrl.Start(ctx); err != nil {
		log.Error(ctx, "start Eventbus Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	//trigger controller
	triggerCtrlStv := trigger.NewController(cfg.GetTriggerConfig(), etcd)
	if err = triggerCtrlStv.Start(); err != nil {
		log.Error(ctx, "start trigger controller fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	etcdStopCh, err := etcd.Start(ctx)
	if err != nil {
		log.Error(ctx, "failed to start etcd", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(),
			errinterceptor.StreamServerInterceptor(),
			memberinterceptor.StreamServerInterceptor(etcd),
			otelgrpc.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(),
			errinterceptor.UnaryServerInterceptor(),
			memberinterceptor.UnaryServerInterceptor(etcd),
			otelgrpc.UnaryServerInterceptor(),
		),
	)

	// for debug in developing stage
	if cfg.GRPCReflectionEnable {
		reflection.Register(grpcServer)
	}

	ctrlpb.RegisterEventBusControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterEventLogControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterSegmentControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterPingServerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterTriggerControllerServer(grpcServer, triggerCtrlStv)
	log.Info(ctx, "the grpc server ready to work", nil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
		wg.Done()
	}()
	select {
	case <-etcdStopCh:
		log.Info(ctx, "received etcd ready to stop, preparing exit", nil)
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	case <-segmentCtrl.StopNotify():
		log.Info(ctx, "received segment controller ready to stop, preparing exit", nil)
	}
	triggerCtrlStv.Stop(ctx)
	segmentCtrl.Stop()
	etcd.Stop(ctx)
	grpcServer.GracefulStop()
	wg.Wait()
	log.Info(ctx, "the controller has been shutdown gracefully", nil)
}

func startMetrics() {
	metrics.RegisterControllerMetrics()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
