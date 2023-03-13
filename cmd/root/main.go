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
	"github.com/vanus-labs/vanus/internal/controller/root"
	"net"
	"os"
	"runtime/debug"
	"sync"

	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"

	"github.com/vanus-labs/vanus/internal/controller"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/vanus-labs/vanus/internal/primitive/interceptor/memberinterceptor"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

var configPath = flag.String("config", "./config/root.yaml",
	"the configuration file of root controller")

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
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetControllerMetrics)
	mem := member.New(cfg.GetClusterConfig())
	if err = mem.Init(ctx); err != nil {
		log.Error(ctx, "failed to init member", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	snowflakeCtrl := root.NewSnowflakeController(cfg.GetSnowflakeConfig(), mem)
	recoveryOpt := recovery.WithRecoveryHandlerContext(
		func(ctx context.Context, p interface{}) error {
			log.Error(ctx, "goroutine panicked", map[string]interface{}{
				log.KeyError: fmt.Sprintf("%v", p),
				"stack":      string(debug.Stack()),
			})
			return status.Errorf(codes.Internal, "%v", p)
		},
	)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			errinterceptor.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recoveryOpt),
			memberinterceptor.UnaryServerInterceptor(mem),
			otelgrpc.UnaryServerInterceptor(),
		),
	)

	if err = snowflakeCtrl.Start(ctx); err != nil {
		log.Error(ctx, "start Snowflake Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// for debug in developing stage
	if cfg.GRPCReflectionEnable {
		reflection.Register(grpcServer)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctrlpb.RegisterSnowflakeControllerServer(grpcServer, snowflakeCtrl)
	ctrlpb.RegisterPingServerServer(grpcServer, snowflakeCtrl)
	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
		wg.Done()
	}()

	if err = mem.Start(ctx); err != nil {
		log.Error(ctx, "failed to start member", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}

	log.Info(ctx, "the grpc server ready to work", nil)

	exit := func() {
		vanus.DestroySnowflake()
		snowflakeCtrl.Stop()
		mem.Stop(ctx)
		grpcServer.GracefulStop()
	}

	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	}
	exit()
	wg.Wait()
	log.Info(ctx, "the root controller has been shutdown gracefully", nil)
}
