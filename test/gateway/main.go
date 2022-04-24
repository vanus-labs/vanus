package main

import (
	"context"
	"flag"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"log"
)

var (
	addr = flag.String("addr", "127.0.0.1:8080", "")
	eb   = flag.String("eb", "test", "")
	num  = flag.Int("num", 100, "")
	size = flag.Int("size", 64, "")
)

func main() {
	ctx := cloudevents.ContextWithTarget(context.Background(), fmt.Sprintf("http://%s/gateway/%s", *addr, *eb))

	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	data := func() string {
		str := ""
		for idx := 0; idx < *size; idx++ {
			str += "a"
		}
		return str
	}()
	for i := 0; i < *num; i++ {
		e := cloudevents.NewEvent()
		e.SetType("com.cloudevents.sample.sent")
		e.SetSource("https://github.com/cloudevents/sdk-go/v2/samples/httpb/sender")
		if err != nil {
			log.Fatalln("")
		}
		_ = e.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
			"id":      i,
			"message": "Hello, World!",
			"data":    data,
		})

		res := c.Send(ctx, e)
		if cloudevents.IsUndelivered(res) {
			log.Printf("Failed to send: %v", res)
		} else {
			var httpResult *cehttp.Result
			cloudevents.ResultAs(res, &httpResult)
			log.Printf("Sent %d with status code %d", i, httpResult.StatusCode)
		}
	}
}
