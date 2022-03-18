package main

import (
	"context"
	"fmt"
	"github.com/linkall-labs/eventbus-go"
)

func main() {
	vrn := fmt.Sprintf("vanus://%s/eventlog/%s?namespace=vanus", "127.0.0.1:2048", "df5aff09-9242-4b47-b9df-cf07c5868e3a")
	r, err := eventbus.OpenLogReader(vrn)
	if err != nil {
		panic(err)
	}
	_, _ = r.Seek(context.Background(), 20, 0)
	es, er := r.Read(context.Background(), 100)
	if err != nil {
		panic(er)
	}
	for i, v := range es {
		println(i, string(v.Data()))
	}
}
