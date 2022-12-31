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
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/linkall-labs/vanus/observability/log"
	v1 "github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/config"
	"github.com/linkall-labs/vanus/internal/store/segment"
)

const (
	blockInfoKeyInRedis = "/vanus/test/store/block_records"
)

var (
	volumeID       int64
	segmentNumbers int
	storeAddrs     []string
	totalSent      int64
	noCleanCache   bool
	replicaNum     int
	blockSize      int64
	batchSize      int
)

func ComponentCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "component",
		Short: "start component test",
	}
	cmd.AddCommand(storeCommand())
	return cmd
}

func storeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "start local store",
	}
	cmd.AddCommand(runStoreCommand())
	cmd.AddCommand(createBlockCommand())
	cmd.AddCommand(sendCommand())
	return cmd
}

func runStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "start local store",
		Run: func(cmd *cobra.Command, args []string) {
			vanus.InitFakeSnowflake()
			_ = os.Setenv("SEGMENT_SERVER_DEBUG_MODE", "true")
			go func() {
				_ = http.ListenAndServe("0.0.0.0:8080", nil)
			}()
			cfg := store.Config{
				IP:   "127.0.0.1",
				Port: 2148 + int(volumeID),
				Volume: store.VolumeInfo{
					ID:       uint16(volumeID),
					Dir:      fmt.Sprintf("/Users/wenfeng/tmp/data/test/vanus/store-%d", uint16(volumeID)),
					Capacity: 1073741824,
				},
				MetaStore: config.SyncStore{
					WAL: config.WAL{
						IO: config.IO{
							Engine: "psync",
						},
					},
				},
				OffsetStore: config.AsyncStore{
					WAL: config.WAL{
						IO: config.IO{
							Engine: "psync",
						},
					},
				},
				Raft: config.Raft{
					WAL: config.WAL{
						BlockSize: 32 * 1024,
						IO: config.IO{
							Engine: "psync",
						},
					},
				},
			}
			runStore(cfg)
		},
	}
	cmd.Flags().Int64Var(&volumeID, "volume-id", 1, "")
	return cmd
}

func createBlockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create-block",
		Short: "create blocks",
		Run: func(cmd *cobra.Command, args []string) {
			addrs := make(map[string]*grpc.ClientConn)
			clis := make([]segpb.SegmentServerClient, len(storeAddrs))
			for idx, addr := range storeAddrs {
				var opts []grpc.DialOption
				opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
				conn, err := grpc.Dial(addr, opts...)
				if err != nil {
					panic("failed to dial to: " + err.Error())
				}
				defer conn.Close()
				addrs[addr] = conn

				clis[idx] = segpb.NewSegmentServerClient(conn)
			}

			cnt := 0
			leaderCnt := 0
			for idx := 0; idx < segmentNumbers; idx++ {
				replicas := map[uint64]string{}
				for j := 0; j < replicaNum; j++ {
					id := vanus.NewTestID()
					cli := clis[cnt%len(clis)]
					_, err := cli.CreateBlock(context.Background(), &segpb.CreateBlockRequest{
						Id:   id.Uint64(),
						Size: blockSize * 1024 * 1024,
					})
					if err != nil {
						panic("failed to create block to: " + err.Error())
					}
					replicas[id.Uint64()] = storeAddrs[cnt%len(clis)]
					cnt++
					if len(replicas) >= replicaNum {
						break
					}
				}
				req := &segpb.ActivateSegmentRequest{
					EventLogId:     vanus.NewTestID().Uint64(),
					ReplicaGroupId: vanus.NewTestID().Uint64(),
					Replicas:       replicas,
				}
				leaderAddr := leaderCnt % len(clis)
				_, err := clis[leaderAddr].ActivateSegment(context.Background(), req)
				if err != nil {
					panic("failed to activate segment to: " + err.Error())
				}
				br := &BlockRecord{
					LeaderID:   0,
					LeaderAddr: storeAddrs[leaderAddr],
					Replicas:   replicas,
				}
				for id, addr := range br.Replicas {
					if addr == br.LeaderAddr {
						br.LeaderID = id
						break
					}
				}
				data, _ := json.Marshal(br)
				rCmd := rdb.LPush(context.Background(), blockInfoKeyInRedis, data)
				if rCmd.Err() != nil {
					fmt.Printf("failed to save block info to redis: %s\n", err)
				}
				fmt.Printf("the segment: %d activated to %s\n", req.ReplicaGroupId, storeAddrs[leaderAddr])
				leaderCnt++
			}

			return
		},
	}
	cmd.Flags().Int64Var(&blockSize, "block-size", 64, "MB")
	cmd.Flags().IntVar(&segmentNumbers, "number", 1, "")
	cmd.Flags().IntVar(&replicaNum, "replicas", 3, "")
	cmd.Flags().StringArrayVar(&storeAddrs, "store-address", []string{
		"127.0.0.1:2149", "127.0.0.1:2150", "127.0.0.1:2151",
	}, "")
	return cmd
}

func sendCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "send",
		Short: "send events to block",
		Run: func(cmd *cobra.Command, args []string) {
			rCmd := rdb.LLen(context.Background(), blockInfoKeyInRedis)
			if rCmd.Err() != nil {
				panic("failed to read block records from redis: " + rCmd.Err().Error())
			}

			le := int(rCmd.Val())
			brs := make([]BlockRecord, le)
			for i := 0; i < le; i++ {
				var sCmd *redis.StringCmd
				if noCleanCache {
					sCmd = rdb.LIndex(context.Background(), blockInfoKeyInRedis, int64(i))
				} else {
					sCmd = rdb.LPop(context.Background(), blockInfoKeyInRedis)
				}
				if rCmd.Err() != nil {
					panic("failed to read block records from redis: " + rCmd.Err().Error())
				}
				br := BlockRecord{}
				_ = json.Unmarshal([]byte(sCmd.Val()), &br)
				brs[i] = br
				fmt.Printf("found a block, ID: %s, addr: %s\n",
					vanus.ID(br.LeaderID).String(), br.LeaderAddr)
			}

			conns := make(map[string]*grpc.ClientConn, 0)
			clis := make(map[string]segpb.SegmentServerClient, 0)
			for _, v := range brs {
				_, exist := conns[v.LeaderAddr]
				if exist {
					continue
				}
				var opts []grpc.DialOption
				opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
				conn, err := grpc.Dial(v.LeaderAddr, opts...)
				if err != nil {
					panic("failed to dial to: " + err.Error())
				}

				conns[v.LeaderAddr] = conn
				clis[v.LeaderAddr] = segpb.NewSegmentServerClient(conn)
			}
			defer func() {
				for _, v := range conns {
					_ = v.Close()
				}
			}()

			count := int64(0)
			failed := int64(0)
			for _, abr := range brs {
				cli := clis[abr.LeaderAddr]
				for idx := 0; idx < parallelism; idx++ {
					go func(br BlockRecord, c segpb.SegmentServerClient) {
						for atomic.LoadInt64(&count)+atomic.LoadInt64(&failed) < totalSent {
							events := generateEvents()
							_, err := c.AppendToBlock(context.Background(), &segpb.AppendToBlockRequest{
								BlockId: br.LeaderID,
								Events:  &v1.CloudEventBatch{Events: events},
							})
							if err != nil {
								atomic.AddInt64(&failed, 1)
								time.Sleep(time.Second)
								fmt.Printf("failed to append events to %s, block [%s], error: [%s]\n",
									br.LeaderAddr, vanus.ID(br.LeaderID).String(), err.Error())
							} else {
								atomic.AddInt64(&count, int64(len(events)))
							}
						}
					}(abr, cli)
				}
			}
			pre := int64(0)
			for atomic.LoadInt64(&count)+atomic.LoadInt64(&failed) < totalSent {
				cur := atomic.LoadInt64(&count)
				fmt.Printf("success: %d, failed: %d, TPS: %d\n",
					cur, atomic.LoadInt64(&failed), cur-pre)
				pre = cur
				time.Sleep(time.Second)
			}
			fmt.Printf("success: %d, failed: %d, TPS: %d\n",
				count, atomic.LoadInt64(&failed), count-pre)
		},
	}
	cmd.Flags().BoolVar(&noCleanCache, "no-clean-cache", false, "")
	cmd.Flags().Int64Var(&totalSent, "total-number", 100000, "")
	cmd.Flags().IntVar(&parallelism, "parallelism", 4, "")
	cmd.Flags().IntVar(&payloadSize, "payload-size", 1024, "")
	cmd.Flags().IntVar(&batchSize, "batch-size", 1, "")
	return cmd
}

func runStore(cfg store.Config) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error(context.Background(), "Listen tcp port failed.", map[string]interface{}{
			log.KeyError: err,
			"port":       cfg.Port,
		})
		os.Exit(-1)
	}

	ctx := context.Background()
	srv := segment.NewServer(cfg)

	if err = srv.Initialize(ctx); err != nil {
		log.Error(ctx, "The SegmentServer has initialized failed.", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}

	log.Info(ctx, "The SegmentServer ready to work.", map[string]interface{}{
		"listen_ip":   cfg.IP,
		"listen_port": cfg.Port,
	})

	if err = srv.Serve(listener); err != nil {
		log.Error(ctx, "The SegmentServer occurred an error.", map[string]interface{}{
			log.KeyError: err,
		})
		return
	}

	log.Info(ctx, "The SegmentServer has been shutdown.", nil)
}

var (
	gOnce sync.Once
	rd    = rand.New(rand.NewSource(time.Now().UnixNano()))
	e     []*v1.CloudEvent
)

func generateEvents() []*v1.CloudEvent {
	gOnce.Do(func() {
		for idx := 0; idx < batchSize; idx++ {
			e = append(e, &v1.CloudEvent{
				Id:          "example-event",
				Source:      "example/uri",
				SpecVersion: "1.0",
				Type:        "example.type",
				Data:        &v1.CloudEvent_TextData{TextData: genStr(rd, payloadSize)},
			})
		}
	})
	return e
}

func startMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		log.Error(context.Background(), "Metrics listen and serve failed.", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
