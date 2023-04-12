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

package command

import (
	"context"
	"encoding/json"
	"fmt"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	client "github.com/vanus-labs/sdk/golang"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	name         string
	eventbusList []string
	number       int64
	parallelism  int
	payloadSize  int
	batchSize    int
	mutex        sync.RWMutex
	payload      string

	port           int
	clientProtocol string
)

func SendCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send SUB-COMMAND",
		Short: "start sending benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			if len(eventbusList) == 0 {
				panic("eventbus list is empty")
			}

			log.Info(context.Background()).Str("name", name).Msg("benchmark")

			sendWithGRPC(cmd)
		},
	}
	cmd.Flags().StringVar(&name, "name", "default", "the task name")
	cmd.Flags().StringArrayVar(&eventbusList, "eventbus", []string{}, "the eventbus name used to")
	cmd.Flags().Int64Var(&number, "number", 100000, "the event number")
	cmd.Flags().IntVar(&parallelism, "parallelism", 1, "")
	cmd.Flags().IntVar(&payloadSize, "payload-size", 64, "byte")
	cmd.Flags().StringVar(&clientProtocol, "protocol", "grpc", "")
	cmd.Flags().IntVar(&batchSize, "batch-size", 1, "")
	return cmd
}

func sendWithGRPC(cmd *cobra.Command) {
	endpoint := mustGetGatewayEndpoint(cmd)

	// start
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var success int64
	wg := sync.WaitGroup{}
	latency := hdrhistogram.New(1, 10000000, 10000)
	c, err := client.Connect(&client.ClientOptions{
		Endpoint: endpoint,
	})
	if err != nil {
		panic("failed to connect to Vanus")
	}

	for _, eb := range eventbusList {
		if err != nil {
			panic("failed to get eventbus")
		}
		for idx := 0; idx < parallelism; idx++ {
			wg.Add(1)
			go func() {
				sender := c.Publisher(client.WithEventbus("default", eb))
				for atomic.LoadInt64(&success) < number {
					s := time.Now()
					events := generateEvents()
					if err := sender.Publish(ctx, events...); err != nil {
						log.Warn(ctx).Err(err).Msg("failed to send events")
					} else {
						atomic.AddInt64(&success, int64(len(events)))
						if err := latency.RecordValue(time.Now().Sub(s).Microseconds()); err != nil {
							panic(err)
						}
					}
				}
				wg.Done()
			}()
		}
	}

	ctx, can := context.WithCancel(context.Background())
	m := make(map[int]int, 0)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		var prev int64
		tick := time.NewTicker(time.Second)
		c := 1
		defer func() {
			tick.Stop()
			tps := success - prev
			log.Info().Msg(fmt.Sprintf("Sent: %d, TPS: %d\n", success, tps))
			m[c] = int(tps)
			wg2.Done()
		}()
		for prev < number {
			select {
			case <-tick.C:
				cur := atomic.LoadInt64(&success)
				tps := cur - prev
				m[c] = int(tps)
				log.Info().Msg(fmt.Sprintf("Sent: %d, TPS: %d\n", success, tps))
				prev = cur
				c++
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	can()
	wg2.Wait()
	averageTPS(m)

	printLatency(latency, "us")
	log.Info().
		Int64("success", success).
		Int64("failed", number-success).
		Dur("used", time.Now().Sub(start)).
		Msg("all message were sent")
}

func averageTPS(m map[int]int) {
	sum := 0
	for _, tps := range m {
		sum += tps
	}
	println("Average TPS: ", sum/len(m))
}

func generateEvents() []*v2.Event {
	mutex.RLock()
	defer mutex.RUnlock()
	if payload == "" {
		mutex.RUnlock()
		mutex.Lock()
		rd := rand.New(rand.NewSource(time.Now().UnixNano()))
		if payload == "" {
			payload = genStr(rd, payloadSize)
		}
		mutex.Unlock()
		mutex.RLock()
	}
	var ee []*v2.Event
	for idx := 0; idx < batchSize; idx++ {
		e := v2.NewEvent()
		e.SetID(uuid.NewString())
		e.SetSource("performance.benchmark.vanus")
		e.SetType("performance.benchmark.vanus")
		e.SetExtension("bornat", time.Now())
		_ = e.SetData(v2.TextPlain, payload)
		ee = append(ee, &e)
	}
	return ee
}

var (
	consumingHis  = hdrhistogram.New(1, 1000, 50)
	totalReceived = int64(0)
)

func ReceiveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "receive",
		Short: "vanus performance benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			ls, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				cmdFailedf(cmd, "init network error: %s", err)
			}

			grpcServer := grpc.NewServer()
			prev := int64(0)
			exit := false
			go func() {
				for !exit {
					cur := atomic.LoadInt64(&totalReceived)
					tps := cur - prev
					prev = cur
					log.Info().Msg(fmt.Sprintf("Received: %d, TPS: %d\n", cur, tps))
					time.Sleep(time.Second)
				}
			}()
			cloudevents.RegisterCloudEventsServer(grpcServer, &testReceiver{})
			log.Info().Str("name", name).Msg("the receiver ready to work")
			go func() {
				err = grpcServer.Serve(ls)
				if err != nil {
					log.Error().Err(err).Msg("grpc server occurred an error")
				}
			}()
			for totalReceived < number {
				time.Sleep(time.Second)
			}
			exit = true
			printLatency(consumingHis, "ms")
		},
	}
	cmd.Flags().IntVar(&port, "port", 8080, "the port the receive server running")
	cmd.Flags().Int64Var(&number, "number", 1000000, "how many events expected to receive")
	return cmd
}

type testReceiver struct{}

func (t testReceiver) Send(_ context.Context, event *cloudevents.BatchEvent) (*emptypb.Empty, error) {
	for idx := range event.Events.GetEvents() {
		e := event.Events.GetEvents()[idx]
		attr := e.GetAttributes()["bornat"]
		bornAt := attr.GetCeTimestamp().AsTime()
		latency := time.Now().Sub(bornAt)
		if err := consumingHis.RecordValue(latency.Milliseconds()); err != nil {
			log.Warn().Err(err).
				Interface("origin ", attr).
				Time("bornAt", bornAt).
				Dur("val", latency).Msg("histogram error")
		}
	}
	atomic.AddInt64(&totalReceived, int64(len(event.Events.GetEvents())))
	return &emptypb.Empty{}, nil
}

func printLatency(his *hdrhistogram.Histogram, unit string) {
	res := his.CumulativeDistribution()
	result := map[string]map[string]interface{}{}
	for _, v := range res {
		if v.Count == 0 {
			continue
		}

		result[fmt.Sprintf("%.2f", v.Quantile)] = map[string]interface{}{
			"value": v.ValueAt,
			"unit":  unit,
			"count": v.Count,
		}
		fmt.Printf("%.2f pct - %d %s\n", v.Quantile, v.ValueAt, unit)
	}

	fmt.Printf("Total: %d\n", his.TotalCount())
	fmt.Printf("Latency Mean: %.2f %s\n", his.Mean(), unit)
	fmt.Printf("Latency StdDev: %.2f\n", his.StdDev())
	fmt.Printf("Latency Max: %d %s, Latency Min: %d %s\n", his.Max(), unit, his.Min(), unit)
	fmt.Println()
}

func isOutputFormatJSON(cmd *cobra.Command) bool {
	v, err := cmd.Flags().GetString("output-format")
	if err != nil {
		return false
	}
	return strings.ToLower(v) == "json"
}

func mustGetGatewayEndpoint(cmd *cobra.Command) string {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	return endpoint
}

func genStr(rd *rand.Rand, size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	data := ""
	for idx := 0; idx < size; idx++ {
		data = fmt.Sprintf("%s%c", data, str[rd.Int31n(int32(len(str)))])
	}
	return data
}

func cmdFailedf(cmd *cobra.Command, format string, a ...interface{}) {
	errStr := format
	if a != nil {
		errStr = fmt.Sprintf(format, a)
	}
	if isOutputFormatJSON(cmd) {
		m := map[string]string{"ERROR": errStr}
		data, _ := json.Marshal(m)
		color.Red(string(data))
	} else {
		t := table.NewWriter()
		t.AppendHeader(table.Row{"ERROR"})
		t.AppendRow(table.Row{errStr})
		t.SetColumnConfigs([]table.ColumnConfig{
			{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
			{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		})
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}

	os.Exit(-1)
}
