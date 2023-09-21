// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package timer

import (
	// standard libraries.
	"context"
	"os"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/signal"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/server/timer/leaderelection"
	"github.com/vanus-labs/vanus/server/timer/timingwheel"
)

func loadConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	Default(c)
	return c, nil
}

func Main(configPath string) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Error().Err(err).Msg("init config error")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()

	MainExt(ctx, *cfg)
}

func MainExt(ctx context.Context, cfg Config) {
	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetTimerMetrics)
	}

	// new leaderelection manager
	leaderelectionMgr := leaderelection.NewLeaderElection(cfg.GetLeaderElectionConfig())
	// new timingwheel manager
	timingwheelMgr := timingwheel.NewTimingWheel(cfg.GetTimingWheelConfig())

	// init timingwheel
	if err := timingwheelMgr.Init(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("init timer wheel failed")
		os.Exit(-1)
	}

	// define leaderelection callback
	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			log.Info(ctx).Msg("leaderelection finish, become leader")
			if timingwheelMgr.IsDeployed(ctx) {
				err := timingwheelMgr.Recover(ctx)
				if err != nil {
					log.Error(ctx).Err(err).Msg("recover for fail-over failed, keeping follower")
					return
				}
			}
			timingwheelMgr.SetLeader(true)
		},
		OnStoppedLeading: func(ctx context.Context) {
			log.Info(ctx).Msg("leaderelection lost, become follower")
			timingwheelMgr.SetLeader(false)
		},
	}

	// start leaderelection
	if err := leaderelectionMgr.Start(ctx, callbacks); err != nil {
		log.Error(ctx).Err(err).Msg("start leader election failed")
		os.Exit(-1)
	}

	// start timingwheel
	if err := timingwheelMgr.Start(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("start timer wheel failed")
		os.Exit(-1)
	}

	select {
	case <-ctx.Done():
		log.Info(ctx).Msg("received system signal, preparing exit")
	case <-timingwheelMgr.StopNotify():
		log.Info(ctx).Msg("received timingwheel manager ready to stop, preparing exit")
		signal.RequestShutdown()
	}

	_ = leaderelectionMgr.Stop(context.Background())
	timingwheelMgr.Stop(context.Background())

	log.Info(ctx).Msg("the timer has been shutdown gracefully")
}
