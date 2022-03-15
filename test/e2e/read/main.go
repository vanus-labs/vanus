package main

import (
	"context"
	"fmt"
	"github.com/linkall-labs/eventbus-go"
)

func main() {
	vrn := fmt.Sprintf("vanus://%s/eventlog/%s?namespace=vanus", "127.0.0.1:2048", "eb947f55-567c-4e1a-b0e3-a905a0a6276d")
	r, err := eventbus.OpenLogReader(vrn)
	if err != nil {
		panic(err)
	}
	_, _ = r.Seek(context.Background(), 3300, 0)
	es, er := r.Read(context.Background(), 10)
	if err != nil {
		panic(er)
	}
	for i, v := range es {
		println(i, string(v.Data()))
	}
}
