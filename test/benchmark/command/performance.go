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
	"errors"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/fatih/color"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	httpPrefix      = "http://"
	eventReceivedAt = "xreceivedat"
	eventSentAt     = "xsentat"
	redisKey        = "/vanus/benchmark/performance"
)

var (
	name         string
	eventbusList []string
	number       int64
	parallelism  int
	payloadSize  int

	port           int
	benchType      string
	clientProtocol string
)

var ebCh = make(chan string, 1024)

func E2ECommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "e2e SUB-COMMAND",
		Short: "run e2e performance test",
	}
	cmd.AddCommand(runCommand())
	cmd.AddCommand(analyseCommand())
	cmd.AddCommand(receiveCommand())
	return cmd
}

func runCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run SUB-COMMAND",
		Short: "vanus performance benchmark program",
		Run: func(cmd *cobra.Command, args []string) {
			if len(eventbusList) == 0 {
				panic("eventbus list is empty")
			}

			log.Info(context.Background(), "benchmark", map[string]interface{}{
				"id": getBenchmarkID(),
			})

			if clientProtocol == "grpc" {
				sendWithGRPC(cmd)
			} else {
				sendWithHTTP(cmd)
			}
		},
	}
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
	cnt := int64(0)
	go func() {
		for atomic.LoadInt64(&cnt) < number {
			for idx := 0; idx < len(eventbusList); idx++ {
				ebCh <- eventbusList[idx]
				atomic.AddInt64(&cnt, 1)
			}
		}
		close(ebCh)
		log.Info(context.Background(), "all events were made", map[string]interface{}{
			"num": number,
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		cmdFailedf(cmd, "failed to connect to gateway")
	}

	batchClient := cloudevents.NewCloudEventsClient(conn)

	var success int64
	wg := sync.WaitGroup{}
	for idx := 0; idx < parallelism; idx++ {
		wg.Add(1)
		go func() {
			for {
				eb, ok := <-ebCh
				if !ok && eb == "" {
					break
				}
				events := generateEvents()
				_, err := batchClient.Send(context.Background(), &cloudevents.BatchEvent{
					EventbusName: eb,
					Events:       &cloudevents.CloudEventBatch{Events: events},
				})
				if err != nil {
					log.Warning(context.Background(), "failed to send events", map[string]interface{}{
						log.KeyError: err,
					})
				} else {
					atomic.AddInt64(&success, int64(len(events)))
				}
			}
			wg.Done()
		}()
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
			log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", success, tps), nil)
			m[c] = int(tps)
			wg2.Done()
		}()
		for prev < number {
			select {
			case <-tick.C:
				cur := atomic.LoadInt64(&success)
				tps := cur - prev
				m[c] = int(tps)
				log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", cur, tps), nil)
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
	saveTPS(m, "produce")
	log.Info(nil, "all message were sent", map[string]interface{}{
		"success": success,
		"failed":  number - success,
		"used":    time.Now().Sub(start),
	})
	_ = rdb.Close()
}

func sendWithHTTP(cmd *cobra.Command) {
	endpoint := mustGetGatewayEndpoint(cmd)

	// start
	start := time.Now()
	cnt := int64(0)
	go func() {
		for atomic.LoadInt64(&cnt) < number {
			for idx := 0; idx < len(eventbusList); idx++ {
				ebCh <- eventbusList[idx]
				atomic.AddInt64(&cnt, 1)
			}
		}
		close(ebCh)
		log.Info(context.Background(), "all events were made", map[string]interface{}{
			"num": number,
		})
	}()

	p, err := ce.NewHTTP()
	if err != nil {
		cmdFailedf(cmd, "init ce protocol error: %s\n", err)
	}
	c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
	if err != nil {
		cmdFailedf(cmd, "create ce client error: %s\n", err)
	}

	var success int64
	wg := sync.WaitGroup{}
	for idx := 0; idx < parallelism; idx++ {
		wg.Add(1)
		go func() {
			for {
				eb, ok := <-ebCh
				if !ok && eb == "" {
					break
				}
				var target string
				if strings.HasPrefix(endpoint, httpPrefix) {
					target = fmt.Sprintf("%s/gateway/%s", endpoint, eb)
				} else {
					target = fmt.Sprintf("%s%s/gateway/%s", httpPrefix, endpoint, eb)
				}
				r, e := send(c, target)
				if e != nil {
					panic(e)
				}
				if r {
					atomic.AddInt64(&success, 1)
				}
			}
			wg.Done()
		}()
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
			log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", success, tps), nil)
			m[c] = int(tps)
			wg2.Done()
		}()
		for prev < number {
			select {
			case <-tick.C:
				cur := atomic.LoadInt64(&success)
				tps := cur - prev
				m[c] = int(tps)
				log.Info(nil, fmt.Sprintf("Sent: %d, TPS: %d\n", cur, tps), nil)
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
	saveTPS(m, "produce")
	log.Info(nil, "all message were sent", map[string]interface{}{
		"success": success,
		"failed":  number - success,
		"used":    time.Now().Sub(start),
	})
	_ = rdb.Close()
}

func saveTPS(m map[int]int, t string) {
	d, _ := json.Marshal(m)
	_ = rdb.Set(context.Background(), getTPSKey(t), d, 0)
}

func getTPSKey(t string) string {
	return path.Join(redisKey, fmt.Sprintf("tps-%s", t), getBenchmarkID())
}

func send(c ce.Client, target string) (bool, error) {
	ctx := ce.ContextWithTarget(context.Background(), target)
	r := &Record{
		BornAt: time.Now(),
	}
	event := ce.NewEvent()

	event.SetID(uuid.NewString())
	event.SetSource("performance.benchmark.vanus")
	event.SetType("performance.benchmark.vanus")
	event.SetTime(r.BornAt)

	err := event.SetData(ce.ApplicationJSON, genData())
	if err != nil {
		return false, errors.New("failed to set data: " + err.Error())
	}
	res := c.Send(ctx, event)
	if ce.IsUndelivered(res) {
		return false, errors.New("failed to send: " + res.Error())
	} else {
		var httpResult *cehttp.Result
		ce.ResultAs(res, &httpResult)
		if httpResult != nil && httpResult.StatusCode == http.StatusOK {
			r.SentAt = time.Now()
			event.SetExtension(eventSentAt, time.Now())
			cache(r, "send")
		} else {
			return false, nil
		}
	}
	return true, nil
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

			c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
			if err != nil {
				cmdFailedf(cmd, "init ce http error: %s", err)
			}
			log.Info(context.TODO(), fmt.Sprintf("the receiver ready to work at %d", port), map[string]interface{}{
				"benchmark_id": getBenchmarkID(),
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
			ch := make(chan *Record, 64)
			wg := sync.WaitGroup{}
			wg.Add(1)
			f := func(his *hdrhistogram.Histogram, unit string) {
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
					fmt.Printf("%.2f pct - %d %s, count: %d\n", v.Quantile, v.ValueAt, unit, v.Count)
				}

				fmt.Printf("Total: %d\n", his.TotalCount())
				fmt.Printf("Latency Mean: %.2f %s\n", his.Mean(), unit)
				fmt.Printf("Latency StdDev: %.2f\n", his.StdDev())
				fmt.Printf("Latency Max: %d %s, Latency Min: %d %s\n", his.Max(), unit, his.Min(), unit)
				fmt.Println()

				r := &BenchmarkResult{
					ID:       primitive.NewObjectID(),
					TaskID:   taskID,
					CaseName: name,
					RType:    ResultLatency,
					Values:   result,
					Mean:     his.Mean(),
					Stdev:    his.StdDev(),
					CreateAt: time.Now(),
				}
				_, err := resultColl.InsertOne(context.Background(), r)
				if err != nil {
					log.Error(nil, "failed to save latency result to mongodb", map[string]interface{}{
						log.KeyError: err,
					})
				}

				tps := hdrhistogram.New(1, 100000, 50)

				cmd := rdb.Get(context.Background(), getTPSKey(benchType))
				m := make(map[int]int, 0)
				_ = json.Unmarshal([]byte(cmd.Val()), &m)
				result = map[string]map[string]interface{}{}
				for idx, v := range m {
					result[fmt.Sprintf("%d", idx)] = map[string]interface{}{
						"value": v,
					}
					if err := tps.RecordValue(int64(v)); err != nil {
						fmt.Println("error: TPS " + err.Error())
					}
				}
				res = tps.CumulativeDistribution()
				for _, v := range res {
					if v.Count == 0 {
						continue
					}

					fmt.Printf("%3.2f pct - %d, count: %d\n", v.Quantile, v.ValueAt, v.Count)
				}
				fmt.Printf("Used: %d s\n", tps.TotalCount())
				fmt.Printf("TPS Mean: %.2f/s\n", tps.Mean())
				fmt.Printf("TPS StdDev: %.2f\n", tps.StdDev())
				fmt.Printf("TPS Max: %d, TPS Min: %d\n", tps.Max(), tps.Min())

				r = &BenchmarkResult{
					ID:       primitive.NewObjectID(),
					TaskID:   taskID,
					CaseName: name,
					RType:    ResultThroughput,
					Values:   result,
					Mean:     tps.Mean(),
					Stdev:    tps.StdDev(),
					CreateAt: time.Now(),
				}
				_, err = resultColl.InsertOne(context.Background(), r)
				if err != nil {
					log.Error(nil, "failed to save latency result to mongodb", map[string]interface{}{
						log.KeyError: err,
					})
				}

				wg.Done()
			}
			dataKey := ""
			if benchType == "produce" {
				dataKey = path.Join(redisKey, "send", getBenchmarkID())
				go analyseProduction(ch, f)
			} else {
				dataKey = path.Join(redisKey, "receive", getBenchmarkID())
				go analyseConsumption(ch, f)
			}
			ctx := context.Background()
			num := rdb.LLen(ctx, dataKey).Val()
			for idx := 0; idx < int(num); idx++ {
				strCMD := rdb.LPop(ctx, dataKey)
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
				data, _ := strCMD.Bytes()
				r := &Record{}
				if err := json.Unmarshal(data, r); err != nil {
					log.Warning(ctx, "unmarshall cloud event failed", map[string]interface{}{
						log.KeyError: err,
						"data":       strCMD.Val(),
					})
				}
				ch <- r
			}
			for len(ch) > 0 {
				time.Sleep(time.Second)
			}
			close(ch)
			wg.Wait()
			_ = rdb.Close()
		},
	}
	cmd.Flags().StringVar(&benchType, "benchmark-type", "", "the type of benchmark, produce or consume")
	return cmd
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
	r := &Record{
		ID:         event.ID(),
		BornAt:     event.Time(),
		ReceivedAt: time.Now(),
	}
	cache(r, "receive")
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

func cache(r *Record, key string) {
	key = path.Join(redisKey, key, getBenchmarkID())
	data, _ := json.Marshal(r)
	cmd := rdb.LPush(context.Background(), key, data)
	if cmd.Err() != nil {
		log.Warning(context.Background(), "set event to redis failed", map[string]interface{}{
			log.KeyError: cmd.Err(),
		})
	}
}

func analyseProduction(ch <-chan *Record, f func(his *hdrhistogram.Histogram, unit string)) {
	his := hdrhistogram.New(1, 10000, 50)
	for r := range ch {
		latency := r.SentAt.Sub(r.BornAt)
		if err := his.RecordValue(latency.Milliseconds()); err != nil {
			fmt.Println(err)
		}
	}
	f(his, "ms")
}

func analyseConsumption(ch <-chan *Record, f func(his *hdrhistogram.Histogram, unit string)) {
	his := hdrhistogram.New(1, 1000, 50)
	cnt := 0
	for r := range ch {
		cnt++
		latency := r.ReceivedAt.Sub(r.BornAt)
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
		go func() {
			rd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				m := map[string]interface{}{
					"data": genStr(rd, payloadSize),
				}
				ch <- m
			}
		}()
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

func getBenchmarkID() string {
	return taskID.Hex()
}
