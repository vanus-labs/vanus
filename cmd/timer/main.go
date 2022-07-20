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
	"os"

	"github.com/linkall-labs/vanus/internal/timer"
	"github.com/linkall-labs/vanus/internal/util/signal"
	"github.com/linkall-labs/vanus/observability/log"
)

var (
	configPath = flag.String("config", "./config/timer.yaml", "the configuration file of timer")
)

func main() {
	flag.Parse()

	cfg, err := timer.InitConfig(*configPath)
	if err != nil {
		log.Error(context.Background(), "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()

	cfg.Init()

	cfg.Start(ctx)

	<-ctx.Done()

	cfg.Stop()

	log.Info(ctx, "the tiemr has been shutdown gracefully", nil)
}
