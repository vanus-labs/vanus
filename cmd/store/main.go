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

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"

	// this project.
	"github.com/vanus-labs/vanus/internal/store"
	"github.com/vanus-labs/vanus/internal/store/block/raw"
	"github.com/vanus-labs/vanus/internal/store/segment"
)

var configPath = flag.String("config", "./config/store.yaml", "store config file path")

func main() {
	flag.Parse()

	cfg, err := store.InitConfig(*configPath)
	if err != nil {
		log.Error().Err(err).Msg("Initialize store config failed.")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Err(err).
			Int("port", cfg.Port).
			Msg("Listen tcp port failed.")
		os.Exit(-1)
	}

	cfg.Observability.T.ServerName = "Vanus Store"
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetSegmentServerMetrics)
	srv := segment.NewServer(*cfg)

	if err = srv.Initialize(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("The SegmentServer has initialized failed.")
		os.Exit(-2)
	}

	log.Info(ctx).
		Str("listen_ip", cfg.IP).
		Int("listen_port", cfg.Port).
		Msg("The SegmentServer ready to work.")

	go func() {
		if err = srv.Serve(listener); err != nil {
			log.Error(ctx).Err(err).Msg("The SegmentServer occurred an error.")
			return
		}
	}()

	if err = srv.RegisterToController(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("failed to register self to controller")
		os.Exit(1)
	}

	select {
	case <-ctx.Done():
		log.Info(ctx).Msg("received system signal, preparing exit")
	}
	raw.CloseAllEngine()
	log.Info(ctx).Msg("The SegmentServer has been shutdown.")
}
