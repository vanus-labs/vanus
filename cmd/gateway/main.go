package main

import (
	"context"
	"github.com/linkall-labs/vanus/internal/gateway"
	"github.com/linkall-labs/vanus/observability/log"
	"os"
)

func main() {
	ga := gateway.NewGateway("127.0.0.1:2048")
	err := ga.StartReceive(context.Background())
	if err != nil {
		log.Error(context.Background(), "start CloudEvents gateway failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
}
