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

package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/linkall-labs/vanus/observability/log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/fatih/color"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

const (
	httpPrefix      = "http://"
	eventReceivedAt = "xreceivedat"
	eventSentAt     = "xsentat"
	redisKey        = "/vanus/benchmark/performance"
)

var (
	number      int64
	eventbusNum int
	parallelism int
	payloadSize int
	eventbus    string

	port        int
	benchmarkID string
	benchType   string
)

func NewPerformanceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "performance SUB-COMMAND",
		Short: "vanus performance benchmark program",
	}
	cmd.AddCommand(runCommand())
	cmd.AddCommand(receiveCommand())
	cmd.AddCommand(analyseCommand())
	return cmd
}

func runCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run SUB-COMMAND",
		Short: "vanus performance benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			endpoint := mustGetGatewayEndpoint(cmd)
			benchmarkID = genStr(rand.New(rand.NewSource(time.Now().UnixNano())), 16)
			log.Info(context.Background(), "benchmark", map[string]interface{}{
				"id": benchmarkID,
			})

			// start
			var success int64
			wg := sync.WaitGroup{}
			wg.Add(eventbusNum * parallelism)
			ctx, cancel := context.WithCancel(context.Background())
			for idx := 1; idx <= eventbusNum; idx++ {
				eb := fmt.Sprintf("performance-%d", idx)
				for p := 0; p < parallelism; p++ {
					go func() {
						run(cmd, endpoint, eb, int(number)/(eventbusNum*parallelism), &success, false)
						wg.Done()
					}()
				}
			}

			go func() {
				var prev int64
				tick := time.NewTicker(time.Second)
				defer tick.Stop()
				for prev < number {
					select {
					case <-tick.C:
						cur := atomic.LoadInt64(&success)
						tps := cur - prev
						log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", cur, tps), nil)
						prev = cur
					case <-ctx.Done():
						return
					}
				}
			}()
			wg.Wait()
			cancel()
			_ = rdb.Close()
		},
	}
	cmd.Flags().Int64Var(&number, "number", 100000, "the event number")
	cmd.Flags().IntVar(&eventbusNum, "eventbus-number", 1, "")
	cmd.Flags().IntVar(&parallelism, "parallelism", 4, "")
	cmd.Flags().IntVar(&payloadSize, "payload-size", 64, "byte")
	cmd.Flags().StringVar(&eventbus, "eventbus", "performance-test", "the eventbus name used to")
	return cmd
}

func receiveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "receive",
		Short: "vanus performance benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			ls, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				cmdFailedf(cmd, "init network error: %s", err)
			}
			benchmarkID = genStr(rand.New(rand.NewSource(time.Now().UnixNano())), 16)

			c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
			if err != nil {
				cmdFailedf(cmd, "init ce http error: %s", err)
			}
			log.Info(context.TODO(), fmt.Sprintf("the receiver ready to work at %d", port), map[string]interface{}{
				"benchmark_id": benchmarkID,
			})
			if err := c.StartReceiver(context.Background(), receive); err != nil {
				cmdFailedf(cmd, "start cloudevents receiver error: %s", err)
			}
		},
	}
	cmd.Flags().IntVar(&port, "port", 8080, "the port the receive server running")
	return cmd
}

func analyseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "analyse",
		Short: "vanus performance benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			if benchType == "" {
				cmdFailedf(cmd, "benchmark-type can't be empty")
			}
			ch := make(chan *ce.Event, 64)
			wg := sync.WaitGroup{}
			wg.Add(1)
			f := func(his *hdrhistogram.Histogram, unit string) {
				res := his.CumulativeDistribution()
				for _, v := range res {
					if v.Count == 0 {
						continue
					}

					fmt.Printf("%.2f pct - %d %s, count: %d\n", v.Quantile, v.ValueAt, unit, v.Count)
				}

				fmt.Printf("Total: %d\n", his.TotalCount())
				fmt.Printf("Mean: %.2f %s\n", his.Mean(), unit)
				fmt.Printf("StdDev: %.2f\n", his.StdDev())
				fmt.Printf("Max: %d %s, Min: %d %s\n", his.Max(), unit, his.Min(), unit)
				fmt.Println()
				wg.Done()
			}
			dataKey := ""
			if benchType == "produce" {
				dataKey = path.Join(redisKey, "send", benchmarkID)
				go analyseProduction(ch, f)
			} else {
				dataKey = path.Join(redisKey, "receive", benchmarkID)
				go analyseConsumption(ch, f)
			}
			ctx := context.Background()
			num := rdb.LLen(ctx, dataKey).Val()
			for idx := 0; idx < int(num); idx++ {
				strCMD := rdb.LIndex(ctx, dataKey, int64(idx))
				if strCMD.Err() != nil {
					if strCMD.Err() == redis.Nil {
						break
					}
					log.Warning(ctx, "LPop failed", map[string]interface{}{
						log.KeyError: strCMD.Err(),
						"key":        dataKey,
					})
					break
				}
				e := ce.NewEvent()
				data, _ := strCMD.Bytes()
				if err := e.UnmarshalJSON(data); err != nil {
					log.Warning(ctx, "unmarshall cloud event failed", map[string]interface{}{
						log.KeyError: err,
						"data":       strCMD.Val(),
					})
				}
				ch <- &e
			}
			for len(ch) > 0 {
				time.Sleep(time.Second)
			}
			close(ch)
			wg.Wait()
			_ = rdb.Close()
		},
	}
	cmd.Flags().StringVar(&benchmarkID, "benchmark-id", "", "the benchmark id to analyse")
	cmd.Flags().StringVar(&benchType, "benchmark-type", "", "the type of benchmark, produce or consume")
	return cmd
}

func run(cmd *cobra.Command, endpoint, eventbus string, number int, cnt *int64, fastFailure bool) {
	p, err := ce.NewHTTP()
	if err != nil {
		cmdFailedf(cmd, "init ce protocol error: %s\n", err)
	}
	c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
	if err != nil {
		cmdFailedf(cmd, "create ce client error: %s\n", err)
	}
	var target string
	if strings.HasPrefix(endpoint, httpPrefix) {
		target = fmt.Sprintf("%s/gateway/%s", endpoint, eventbus)
	} else {
		target = fmt.Sprintf("%s%s/gateway/%s", httpPrefix, endpoint, eventbus)
	}

	failed := 0
	for idx := 0; idx < number; idx++ {
		ctx := ce.ContextWithTarget(context.Background(), target)
		event := ce.NewEvent()

		event.SetID(uuid.NewString())
		event.SetSource("performance.benchmark.vanus")
		event.SetType("performance.benchmark.vanus")
		event.SetTime(time.Now())

		err = event.SetData(ce.ApplicationJSON, genData())
		if err != nil {
			cmdFailedf(cmd, "set data failed: %s\n", err)
		}
		res := c.Send(ctx, event)
		if ce.IsUndelivered(res) {
			cmdFailedf(cmd, "failed to send: %s\n", res.Error())
		} else {
			var httpResult *cehttp.Result
			ce.ResultAs(res, &httpResult)
			if httpResult == nil {
				failed++
			} else if httpResult.StatusCode != http.StatusOK {
				failed++
			} else {
				event.SetExtension(eventSentAt, time.Now())
				cache(&event, "send")
				atomic.AddInt64(cnt, 1)
			}
		}
	}

	if fastFailure && failed != 0 {
		cmdFailedf(cmd, "send failed number: %d\n", failed)
	} else {
		fmt.Printf("%s sent done, failed number: %d\n", eventbus, failed)
	}
}

var receiveOnce = sync.Once{}
var consumingCnt = int64(0)

func receive(_ context.Context, event ce.Event) protocol.Result {
	receiveOnce.Do(func() {
		prev := int64(0)
		go func() {
			for {
				cur := atomic.LoadInt64(&consumingCnt)
				tps := cur - prev
				prev = cur
				log.Info(nil, fmt.Sprintf("Received: %d, TPS: %d\n", cur, tps), nil)
				time.Sleep(time.Second)
			}
		}()
	})
	event.SetExtension(eventReceivedAt, time.Now())
	cache(&event, "receive")
	atomic.AddInt64(&consumingCnt, 1)
	return ce.ResultACK
}

func isOutputFormatJSON(cmd *cobra.Command) bool {
	v, err := cmd.Flags().GetString("output-format")
	if err != nil {
		return false
	}
	return strings.ToLower(v) == "json"
}

var rdb = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

func cache(e *ce.Event, key string) {
	data, _ := e.MarshalJSON()
	cmd := rdb.LPush(context.Background(), path.Join(redisKey, key, benchmarkID), data)
	if cmd.Err() != nil {
		log.Warning(context.Background(), "set event to redis failed", map[string]interface{}{
			log.KeyError: cmd.Err(),
		})
	}
}

func analyseProduction(ch <-chan *ce.Event, f func(his *hdrhistogram.Histogram, unit string)) {
	his := hdrhistogram.New(1, 100, 50)
	for e := range ch {
		sentAtStr := e.Extensions()["xsentat"].(string)
		t, _ := time.Parse(time.RFC3339, sentAtStr)

		latency := t.Sub(e.Time())
		_ = his.RecordValue(latency.Microseconds())
	}
	f(his, "us")
}

func analyseConsumption(ch <-chan *ce.Event, f func(his *hdrhistogram.Histogram, unit string)) {
	his := hdrhistogram.New(1, 1000, 50)
	cnt := 0
	for e := range ch {
		cnt++
		receiveAtStr := e.Extensions()[eventReceivedAt].(string)
		t, _ := time.Parse(time.RFC3339, receiveAtStr)
		latency := t.Sub(e.Time())
		if err := his.RecordValue(latency.Milliseconds()); err != nil {
			log.Warning(context.Background(), "histogram error", map[string]interface{}{
				log.KeyError: err,
				"val":        latency.Milliseconds(),
			})
		}
	}
	f(his, "ms")
}

func mustGetGatewayEndpoint(cmd *cobra.Command) string {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	return endpoint
}

var (
	once = sync.Once{}
	ch   = make(chan map[string]interface{}, 512)
)

func genData() map[string]interface{} {
	once.Do(func() {
		for idx := 0; idx < eventbusNum; idx++ {
			go func() {
				rd := rand.New(rand.NewSource(time.Now().UnixNano()))
				for {
					m := map[string]interface{}{
						"data": genStr(rd, payloadSize),
					}
					ch <- m
				}
			}()
		}
	})
	return <-ch
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
