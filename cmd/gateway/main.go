package main

import (
	"context"
	"github.com/linkall-labs/vanus/internal/gateway"
	"github.com/linkall-labs/vanus/observability/log"
)

func main() {
	ga := gateway.NewGateway("localhost")
	err := ga.StartReceive(context.Background())
	if err != nil {
		log.Fatal("start CloudEvents gateway failed", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
