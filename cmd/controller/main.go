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
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller"
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/grpc_error"
	"github.com/linkall-labs/vanus/internal/primitive/interceptor/grpc_member"
	"github.com/linkall-labs/vanus/internal/util/signal"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"google.golang.org/grpc"
	"net"
	"os"
)

func main() {
	ctx := signal.SetupSignalContext()
	f := flag.String("conf", "./config/controller.yaml", "controller config file path")
	flag.Parse()
	cfg, err := controller.Init(*f)
	if err != nil {
		log.Error(ctx, "init config error", map[string]interface{}{log.KeyError: err})
		os.Exit(-1)
	}
	etcd := embedetcd.New()
	if err = etcd.Init(ctx, cfg.GetEtcdConfig()); err != nil {
		log.Error(ctx, "failed to init etcd", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	if _, err := etcd.Start(ctx); err != nil {
		log.Error(ctx, "failed to start etcd", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// TODO wait server ready
	ctrlSrv := eventbus.NewEventBusController(cfg.GetEventbusCtrlConfig(), etcd)
	if err = ctrlSrv.Start(ctx); err != nil {
		log.Error(ctx, "start Eventbus Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	//trigger controller
	triggerCtrlStv := trigger.NewTriggerController(cfg.TriggerConfig)
	if err = triggerCtrlStv.Start(); err != nil {
		log.Error(ctx, "start trigger controller fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	listen, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.IP, cfg.Port))
	if err != nil {
		log.Error(ctx, "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainStreamInterceptor(
			grpc_error.StreamServerInterceptor(),
			grpc_member.StreamServerInterceptor(etcd, cfg.Topology),
			recovery.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			grpc_error.UnaryServerInterceptor(),
			grpc_member.UnaryServerInterceptor(etcd, cfg.Topology),
			recovery.UnaryServerInterceptor(),
		),
	)
	ctrlpb.RegisterEventBusControllerServer(grpcServer, ctrlSrv)
	ctrlpb.RegisterEventLogControllerServer(grpcServer, ctrlSrv)
	ctrlpb.RegisterSegmentControllerServer(grpcServer, ctrlSrv)
	ctrlpb.RegisterTriggerControllerServer(grpcServer, triggerCtrlStv)
	log.Info(ctx, "the grpc server ready to work", nil)
	err = grpcServer.Serve(listen)
	if err != nil {
		log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		<-ctx.Done()
		grpcServer.GracefulStop()
		triggerCtrlStv.Close()
		log.Info(ctx, "the grpc server has been shutdown", nil)
	}
}
