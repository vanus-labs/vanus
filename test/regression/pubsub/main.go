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
	"github.com/vanus-labs/vanus/observability/log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/cloudevents/sdk-go/v2"

	vanus "github.com/vanus-labs/sdk/golang"
	"github.com/vanus-labs/vanus/test/utils"
)

var (
	sentSuccess    int64
	sentFailed     int64
	receivedNumber int64

	totalNumber        = int64(1 << 20) // 1024*1024
	parallelism        = 4
	batchSize          = 8
	maximumPayloadSize = 4 * 1024
	receiveEventPort   = 18080
	waitTimeout        = 30 * time.Second
	totalLatency       = int64(0)
	caseName           = "vanus.regression.pubsub"

	eventbus = os.Getenv("EVENTBUS")
	endpoint = os.Getenv("VANUS_GATEWAY")

	mutex   = sync.Mutex{}
	cache   = map[string]*v2.Event{}
	unmatch = map[string]*v2.Event{}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	client, err := vanus.Connect(&vanus.ClientOptions{Endpoint: endpoint})
	if err != nil {
		panic(err)
	}

	go receiveEvents(ctx, client)
	utils.PrintTotal(ctx, map[string]*int64{
		"Sending":   &sentSuccess,
		"Receiving": &receivedNumber,
	})

	publishEvents(ctx, client)
	now := time.Now()
	for time.Since(now) < waitTimeout {
		mutex.Lock()
		if len(cache) == 0 {
			mutex.Unlock()
			break
		}
		for k := range unmatch {
			delete(cache, k)
		}
		mutex.Unlock()
		time.Sleep(time.Second)
	}
	cancel()

	if len(cache) != 0 {
		log.Error().Str("case", caseName).
			Int64("success", sentSuccess).
			Int64("received", receivedNumber).
			Int("lost", len(cache)).
			Msg("failed to run pubsub case because of timeout after finished sending")

		log.Info().Msg("lost events are below")
		for _, v := range cache {
			println(v.String())
		}
		os.Exit(1)
	}
	log.Info().Int64("success", sentSuccess).
		Int64("sent_failed", sentFailed).
		Int64("received", receivedNumber).
		Msg("success to run publish and receiveEvents case")
}

func publishEvents(ctx context.Context, client vanus.Client) {
	wg := sync.WaitGroup{}
	start := time.Now()

	for p := 0; p < parallelism; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publisher := client.Publisher(vanus.WithEventbus("default", eventbus))
			ch := utils.CreateEventFactory(ctx, caseName, maximumPayloadSize)
			for atomic.LoadInt64(&sentSuccess) < totalNumber {
				events := make([]*v2.Event, batchSize)
				for n := 0; n < batchSize; n++ {
					events[n] = <-ch
				}
				err := publisher.Publish(context.Background(), events...)
				if err != nil {
					log.Warn().Err(err).Msg("failed to send events")
					atomic.AddInt64(&sentFailed, int64(len(events)))
				} else {
					mutex.Lock()
					for _, e := range events {
						cache[e.ID()] = e
					}
					mutex.Unlock()
					atomic.AddInt64(&sentSuccess, int64(len(events)))
				}
			}
		}()
	}
	wg.Wait()

	log.Info(ctx).
		Int64("success", sentSuccess).
		Dur("used", time.Now().Sub(start)).
		Int64("failed", sentFailed).Msg("finished to sent all events")
}

func receiveEvents(ctx context.Context, c vanus.Client) {
	subscriber := c.Subscriber(
		vanus.WithListenPort(receiveEventPort),
		vanus.WithParallelism(8),
		vanus.WithProtocol(vanus.ProtocolGRPC),
		vanus.WithMaxBatchSize(32),
	)

	err := subscriber.Listen(func(ctx context.Context, msgs ...vanus.Message) error {
		mutex.Lock()
		defer mutex.Unlock()
		for _, msg := range msgs {
			e := msg.GetEvent()
			ce, exist := cache[e.ID()]
			if !exist {
				unmatch[e.ID()] = e
				log.Warn(ctx).Stringer("event", e).
					Msg("received a event, but which isn't found in cache")
				continue
			}
			if string(ce.Data()) != string(e.Data()) {
				log.Error().Msg("received a event, but data was corrupted")
				os.Exit(1)
			}
			delete(cache, e.ID())
			atomic.AddInt64(&totalLatency, ce.Time().UnixMicro())
			msg.Success()
		}
		atomic.AddInt64(&receivedNumber, int64(len(msgs)))
		return nil
	})
	if err != nil {
		log.Error(ctx).Err(err).Msg("failed to start events listening")
	}
}
