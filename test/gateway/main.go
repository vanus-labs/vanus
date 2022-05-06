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
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/linkall-labs/eventbus-go"
	"io"
	"log"
)

var (
	mode      = flag.String("mode", "send", "")
	addr      = flag.String("addr", "127.0.0.1:8080", "")
	eb        = flag.String("eb", "test", "")
	eventType = flag.String("type", "a", "")
	num       = flag.Int("num", 100, "")
	size      = flag.Int("size", 64, "")
	offset    = flag.Int("offset", 0, "")
)

func main() {
	flag.Parse()
	fmt.Printf("params: %s=%s\n", "mode", *mode)
	fmt.Printf("params: %s=%s\n", "addr", *addr)
	fmt.Printf("params: %s=%s\n", "eb", *eb)
	fmt.Printf("params: %s=%d\n", "num", *num)
	fmt.Printf("params: %s=%d\n", "size", *size)
	fmt.Printf("params: %s=%d\n", "offset", *offset)
	if *mode == "send" {
		sender()
	} else if *mode == "receive" {
		receiver()
	}
}

func sender() {
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
		e.SetType(*eventType)
		e.SetSource("gw-util")
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
			log.Printf("Sent %d with status code %d, body: %s", i, httpResult.StatusCode, httpResult.Error())
		}
	}
}

func receiver() {
	vrn := fmt.Sprintf("vanus://%s/eventbus/%s", *addr, *eb)
	ls, err := eventbus.LookupReadableLogs(context.Background(), vrn)
	if err != nil {
		log.Fatal(err)
	}

	r, err := eventbus.OpenLogReader(ls[0].VRN)
	if err != nil {
		log.Fatal(err)
	}

	_, err = r.Seek(context.Background(), int64(*offset), io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}

	events, err := r.Read(context.Background(), int16(*num))
	if err != nil {
		log.Fatal(err)
	}

	if len(events) == 0 {
		log.Printf("no event\n")
		return
	}

	for i, e := range events {
		log.Printf("event %d: \n%s", i, e)
	}

	r.Close()
}
