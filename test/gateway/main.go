package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"log"
)

func main() {
	ctx := cloudevents.ContextWithTarget(context.Background(), "http://localhost:8080/gateway/wwf-0314-3")

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
		for idx := 0; idx < 40960; idx++ {
			str += "a"
		}
		return str
	}()
	for i := 0; i < 1600; i++ {
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
