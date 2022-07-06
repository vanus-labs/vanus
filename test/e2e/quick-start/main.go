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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	log "k8s.io/klog/v2"

	"github.com/fatih/color"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// cloudEventDataRowLength = 4
	httpPrefix       = "http://"
	eventBus         = "quick-start"
	sink             = "http://quick-display:80"
	source           = ""
	filters          = ""
	inputTransformer = ""

	eventID     = "1"
	eventType   = "examples"
	eventBody   = "Hello Vanus"
	eventSource = "quick-start"
)

var (
	httpClient = resty.New()
	endpoint   = os.Getenv("VANUS_GATEWAY")
	err        error
)

func mustGetControllerProxyConn(ctx context.Context) *grpc.ClientConn {
	splits := strings.Split(os.Getenv("VANUS_GATEWAY"), ":")
	port, err := strconv.Atoi(splits[1])
	if err != nil {
		fmt.Println("parsing gateway port failed")
		return nil
	}
	leaderConn := createGRPCConn(ctx, fmt.Sprintf("%s:%d", splits[0], port+2))
	if leaderConn == nil {
		fmt.Println("failed to connect to gateway")
		return nil
	}
	return leaderConn
}

func createGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	addr = strings.TrimPrefix(addr, "http://")
	var err error
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithCancel(ctx)
	timeout := false
	go func() {
		ticker := time.Tick(time.Second)
		select {
		case <-ctx.Done():
		case <-ticker:
			cancel()
			timeout = true
		}
	}()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	cancel()
	if timeout {
		color.Yellow("dial to controller: %s timeout, try to another controller", addr)
		return nil
	} else if err != nil {
		color.Red("dial to controller: %s failed", addr)
		return nil
	}
	return conn
}

func createEventbus(eb string) error {
	ctx := context.Background()
	grpcConn := mustGetControllerProxyConn(ctx)
	defer func() {
		_ = grpcConn.Close()
	}()

	cli := ctrlpb.NewEventBusControllerClient(grpcConn)
	res, err := cli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		Name: eb,
	})
	if err != nil {
		log.Errorf("create eventbus failed, err: %s", err)
		return err
	}
	log.Infof("create eventbus[%s] success.", res.Name)
	return nil
}

func createSubscription(eventbus, sink, source, filters, inputTransformer string) error {
	ctx := context.Background()
	grpcConn := mustGetControllerProxyConn(ctx)
	defer func() {
		_ = grpcConn.Close()
	}()
	var filter []*meta.Filter
	if filters != "" {
		err := json.Unmarshal([]byte(filters), &filter)
		if err != nil {
			log.Errorf("the filter invalid, err: %s", err)
			return err
		}
	}

	var inputTrans *meta.InputTransformer
	if inputTransformer != "" {
		err := json.Unmarshal([]byte(inputTransformer), &inputTrans)
		if err != nil {
			log.Errorf("the inputTransformer invalid, err: %s", err)
			return err
		}
	}

	cli := ctrlpb.NewTriggerControllerClient(grpcConn)
	res, err := cli.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
		Source:           source,
		Filters:          filter,
		Sink:             sink,
		EventBus:         eventbus,
		InputTransformer: inputTrans,
	})
	if err != nil {
		log.Errorf("create subscription failed, err: %s", err)
		return err
	}
	log.Infof("create subscription[%d] success.", res.Id)
	return nil
}

func putEvent(eventbus, eventID, eventType, eventBody, eventSource string) error {
	p, err := ce.NewHTTP()
	if err != nil {
		log.Errorf("init ce protocol error: %s\n", err)
		return err
	}
	c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
	if err != nil {
		log.Errorf("create ce client error: %s\n", err)
		return err
	}

	if eventID == "" {
		eventID = uuid.NewString()
	}

	ceCtx := ce.ContextWithTarget(context.Background(), fmt.Sprintf("%s%s/gateway/%s", httpPrefix, endpoint, eventbus))
	event := ce.NewEvent()
	event.SetID(eventID)
	event.SetSource(eventSource)
	event.SetType(eventType)
	err = event.SetData(ce.TextPlain, eventBody)
	if err != nil {
		log.Errorf("set data failed: %s\n", err)
		return err
	}
	c.Send(ceCtx, event)
	log.Infof("put event[%s] success.", event.ID())
	return nil
}

func putEvents(eventNum, threadNum int64) error {
	var (
		i       int64 = 1
		eventid int64 = 1
		wg      sync.WaitGroup
	)
	for i = 1; i <= threadNum; i++ {
		first := eventid
		last := i * eventNum / threadNum
		wg.Add(1)
		go func(first, last int64) {
			fmt.Printf("first: %d, last: %d\n", first, last)
			for n := first; n <= last; n++ {
				putEvent(eventBus, fmt.Sprintf("%d", n), eventType, eventBody, eventSource)
			}
			wg.Done()
		}(first, last)
		eventid = eventid + eventNum/threadNum
	}
	wg.Wait()
	return nil
}

func getEvent(eventbus, offset, number string) error {
	idx := strings.LastIndex(endpoint, ":")
	port, err := strconv.Atoi(endpoint[idx+1:])
	if err != nil {
		log.Errorf("parse gateway port failed: %s, endpoint: %s", err, endpoint)
		return err
	}
	newEndpoint := fmt.Sprintf("%s:%d", endpoint[:idx], port+1)
	url := fmt.Sprintf("%s%s/getEvents?eventbus=%s&offset=%s&number=%s", httpPrefix, newEndpoint, eventbus, offset, number)
	evevt, err := httpClient.NewRequest().Get(url)
	if err != nil {
		log.Errorf("get event failed, err: %s\n", err)
		return err
	}
	log.Infof("get event success, event: %s\n", evevt.String())
	return nil
}

func Test_e2e_base() {
	err = createEventbus(eventBus)
	if err != nil {
		return
	}

	err = createSubscription(eventBus, sink, source, filters, inputTransformer)
	if err != nil {
		return
	}

	putEvents(10000, 100)

	err = getEvent(eventBus, "0", "10000")
	if err != nil {
		return
	}
}

func Test_e2e_filter() {}

func Test_e2e_transformation() {}

func main() {
	log.Info("start e2e test base case...")

	Test_e2e_base()

	Test_e2e_filter()

	Test_e2e_transformation()

	log.Info("finish e2e test base case...")
}

