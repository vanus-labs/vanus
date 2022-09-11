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
	// standard libraries.
	"context"
	"flag"
	"fmt"
	"github.com/linkall-labs/vanus/observability/tracing"
	"net"
	"net/http"
	"os"

	// this project.
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/segment"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var configPath = flag.String("config", "./config/store.yaml", "store config file path")

func main() {
	flag.Parse()

	cfg, err := store.InitConfig(*configPath)
	if err != nil {
		log.Error(context.Background(), "Initialize store config failed.", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	tracing.Init("Vanus-Store")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error(context.Background(), "Listen tcp port failed.", map[string]interface{}{
			log.KeyError: err,
			"port":       cfg.Port,
		})
		os.Exit(-1)
	}

	metrics.RegisterSegmentServerMetrics()
	go startMetrics()

	ctx := context.Background()
	srv := segment.NewServer(*cfg)

	if err = srv.Initialize(ctx); err != nil {
		log.Error(ctx, "The SegmentServer has initialized failed.", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}

	log.Info(ctx, "The SegmentServer ready to work.", map[string]interface{}{
		"listen_ip":   cfg.IP,
		"listen_port": cfg.Port,
	})

	if err = srv.Serve(listener); err != nil {
		log.Error(ctx, "The SegmentServer occurred an error.", map[string]interface{}{
			log.KeyError: err,
		})
		return
	}

	log.Info(ctx, "The SegmentServer has been shutdown.", nil)
}

func startMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		log.Error(context.Background(), "Metrics listen and serve failed.", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
