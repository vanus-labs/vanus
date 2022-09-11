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
	"github.com/linkall-labs/vanus/pkg/util/signal"
	"net/http"
	"os"

	"github.com/linkall-labs/vanus/internal/timer"
	"github.com/linkall-labs/vanus/internal/timer/leaderelection"
	"github.com/linkall-labs/vanus/internal/timer/timingwheel"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	configPath = flag.String("config", "./config/timer.yaml", "the configuration file of timer")
)

func main() {
	var (
		err error
		ctx context.Context
	)

	flag.Parse()
	ctx = signal.SetupSignalContext()
	cfg, err := timer.InitConfig(*configPath)
	if err != nil {
		log.Error(ctx, "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	go startMetrics(cfg)

	// new leaderelection manager
	leaderelectionMgr := leaderelection.NewLeaderElection(cfg.GetLeaderElectionConfig())
	// new timingwheel manager
	timingwheelMgr := timingwheel.NewTimingWheel(cfg.GetTimingWheelConfig())

	// init timingwheel
	if err = timingwheelMgr.Init(ctx); err != nil {
		log.Error(ctx, "init timer wheel failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// define leaderelection callback
	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			log.Info(ctx, "leaderelection finish, become leader", nil)
			if timingwheelMgr.IsDeployed(ctx) {
				err := timingwheelMgr.Recover(ctx)
				if err != nil {
					log.Error(ctx, "recover for failover failed, keeping follower", map[string]interface{}{
						log.KeyError: err,
					})
					return
				}
			}
			timingwheelMgr.SetLeader(true)
		},
		OnStoppedLeading: func(ctx context.Context) {
			log.Info(ctx, "leaderelection lost, become follower", nil)
			timingwheelMgr.SetLeader(false)
		},
	}

	// start leaderelection
	if err = leaderelectionMgr.Start(ctx, callbacks); err != nil {
		log.Error(ctx, "start leader election failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// start timingwheel
	if err = timingwheelMgr.Start(ctx); err != nil {
		log.Error(ctx, "start timer wheel failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	case <-timingwheelMgr.StopNotify():
		log.Info(ctx, "received timingwheel manager ready to stop, preparing exit", nil)
		signal.RequestShutdown()
	}

	leaderelectionMgr.Stop(context.Background())
	timingwheelMgr.Stop(context.Background())

	log.Info(ctx, "the tiemr has been shutdown gracefully", nil)
}

func startMetrics(cfg *timer.Config) {
	metrics.RegisterTimerMetrics()
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		log.Error(context.Background(), "Metrics listen and serve failed.", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
