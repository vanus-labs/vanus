// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except compliance with the License.
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
	"fmt"
	"github.com/linkall-labs/sdk/proto/pkg/vanus"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkall-labs/vanus/observability/log"
	pkgutil "github.com/linkall-labs/vanus/pkg/util"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"github.com/linkall-labs/vanus/test/utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	sentSuccess    int64
	sentFailed     int64
	receivedNumber int64

	totalNumber        = int64(1 << 17) // 1024*1024
	parallelism        = 4
	batchSize          = 8
	maximumPayloadSize = 4 * 1024
	receiveEventPort   = 18080
	waitTimeout        = 30 * time.Second
	totalLatency       = int64(0)
	caseName           = "vanus.regression.pubsub"

	eventbus = os.Getenv("EVENTBUS")
	endpoint = os.Getenv("VANUS_GATEWAY")

	cache = sync.Map{}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go subscribe(ctx, &receiver{}, receiveEventPort)
	utils.PrintTotal(ctx, map[string]*int64{
		"Sending":   &sentSuccess,
		"Receiving": &receivedNumber,
	})

	publishEvents(ctx)
	now := time.Now()
	for time.Since(now) < waitTimeout && pkgutil.MapLen(&cache) > 0 {
		time.Sleep(time.Second)
	}
	cancel()
	if pkgutil.MapLen(&cache) != 0 {
		log.Error(ctx, "failed to run pubsub case because of timeout after finished sending", map[string]interface{}{
			"success":  sentSuccess,
			"received": receivedNumber,
			"lost":     pkgutil.MapLen(&cache),
		})
		os.Exit(1)
	}
	log.Info(ctx, "success to run publish and subscribe case", map[string]interface{}{
		"success":     sentSuccess,
		"sent_failed": sentFailed,
		"received":    receivedNumber,
	})
}

func publishEvents(ctx context.Context) {
	wg := sync.WaitGroup{}
	start := time.Now()
	for p := 0; p < parallelism; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchClient := utils.NewClient(ctx, endpoint)
			ch := utils.CreateEventFactory(ctx, caseName, maximumPayloadSize)
			for atomic.LoadInt64(&sentSuccess) < totalNumber {
				events := make([]*cloudevents.CloudEvent, batchSize)
				for n := 0; n < batchSize; n++ {
					events[n] = <-ch
				}
				_, err := batchClient.Publish(context.Background(), &vanus.PublishRequest{
					EventbusName: eventbus,
					Events:       &cloudevents.CloudEventBatch{Events: events},
				})
				if err != nil {
					log.Warning(context.Background(), "failed to send events", map[string]interface{}{
						log.KeyError: err,
					})
					atomic.AddInt64(&sentFailed, int64(len(events)))
				} else {
					for _, e := range events {
						cache.Store(e.Id, e)
					}
					atomic.AddInt64(&sentSuccess, int64(len(events)))
				}
			}
		}()
	}
	wg.Wait()

	log.Info(ctx, "finished to sent all events", map[string]interface{}{
		"success": sentSuccess,
		"failed":  sentFailed,
		"used":    time.Now().Sub(start),
	})
}

func subscribe(ctx context.Context, srv cloudevents.CloudEventsServer, port int) {
	ls, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Error(ctx, "failed to send sending finished signal", map[string]interface{}{
			"success":  sentSuccess,
			"received": receivedNumber,
		})
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	cloudevents.RegisterCloudEventsServer(grpcServer, srv)
	err = grpcServer.Serve(ls)
	if err != nil {
		log.Error(nil, "grpc server occurred an error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(1)
	}
}

type receiver struct {
}

func (r *receiver) Send(ctx context.Context, event *cloudevents.BatchEvent) (*emptypb.Empty, error) {
	for _, e := range event.Events.Events {
		v, exist := cache.LoadAndDelete(e.Id)
		if !exist {
			log.Error(ctx, "received a unexpected event, which ID isn't found", nil)
			os.Exit(1)
		}
		ce := v.(*cloudevents.CloudEvent)
		if ce.GetTextData() != string(e.GetBinaryData()) {
			log.Error(ctx, "received a event, but data was corrupted", nil)
			os.Exit(1)
		}
		atomic.AddInt64(&totalLatency, ce.Attributes["time"].GetCeTimestamp().AsTime().UnixMicro())
	}
	atomic.AddInt64(&receivedNumber, int64(len(event.Events.Events)))
	return &emptypb.Empty{}, nil
}
