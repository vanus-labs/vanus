package main

import (
	"context"
	"flag"
	"github.com/linkall-labs/vanus/internal/gateway"
	"github.com/linkall-labs/vanus/observability/log"
	"os"
)

func main() {
	f := flag.String("config", "./config/gateway.yaml", "gateway config file path")
	flag.Parse()
	cfg, err := gateway.InitConfig(*f)
	if err != nil {
		log.Error(nil, "init config error", map[string]interface{}{log.KeyError: err})
		os.Exit(-1)
	}
	ga := gateway.NewGateway(*cfg)
	err = ga.StartReceive(context.Background())
	if err != nil {
		log.Error(context.Background(), "start CloudEvents gateway failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
}
