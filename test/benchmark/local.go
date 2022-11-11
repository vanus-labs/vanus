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
	cloudevents "cloudevents.io/genproto/v1"
	"context"
	"encoding/json"
	"fmt"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/segment"
	"github.com/linkall-labs/vanus/observability/log"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"net/http"
	"os"
)

const (
	blockInfoKeyInRedis = "/vanus/test/store/block_records"
)

var (
	volumeID       int64
	segmentNumbers int
	storeAddrs     []string
)

func localCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "local",
		Short: "start local server",
	}
	cmd.AddCommand(storeCommand())
	return cmd
}

func storeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "start local store",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			initRedis()
		},
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

			cfg := store.Config{
				IP:   "127.0.0.1",
				Port: 2148 + int(volumeID),
				Volume: store.VolumeInfo{
					ID:       uint16(volumeID),
					Dir:      fmt.Sprintf("/Users/wenfeng/tmp/data/test/vanus/store-%d", uint16(volumeID)),
					Capacity: 1073741824,
				},
				MetaStore: store.SyncStoreConfig{
					WAL: store.WALConfig{
						IO: store.IOConfig{
							Engine: "psync",
						},
					},
				},
				OffsetStore: store.AsyncStoreConfig{
					WAL: store.WALConfig{
						IO: store.IOConfig{
							Engine: "psync",
						},
					},
				},
				Raft: store.RaftConfig{
					WAL: store.WALConfig{
						IO: store.IOConfig{
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
			blocks := map[int][]uint64{}
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
				blocks[idx] = make([]uint64, 0)
			}

			for idx := 0; idx < segmentNumbers; idx++ {
				replicas := map[uint64]string{}
				for j, cli := range clis {
					id := vanus.NewTestID()
					_, err := cli.CreateBlock(context.Background(), &segpb.CreateBlockRequest{
						Id:   id.Uint64(),
						Size: 64 * 1024 * 1024,
					})
					if err != nil {
						panic("failed to create block to: " + err.Error())
					}
					replicas[id.Uint64()] = storeAddrs[j]
					blocks[j] = append(blocks[j], id.Uint64())
				}
				req := &segpb.ActivateSegmentRequest{
					EventLogId:     vanus.NewTestID().Uint64(),
					ReplicaGroupId: vanus.NewTestID().Uint64(),
					Replicas:       replicas,
				}
				_, err := clis[idx%len(clis)].ActivateSegment(context.Background(), req)
				if err != nil {
					panic("failed to activate segment to: " + err.Error())
				}

				br := &BlocKRecord{
					LeaderID:   0,
					LeaderAddr: storeAddrs[idx%len(clis)],
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
				fmt.Printf("the segment: %d activated to %s\n", req.ReplicaGroupId, storeAddrs[idx%len(clis)])
			}

			return
		},
	}
	cmd.Flags().IntVar(&segmentNumbers, "number", 1, "")
	cmd.Flags().StringArrayVar(&storeAddrs, "store-address", []string{
		"127.0.0.1:2149", "127.0.0.1:2150", "127.0.0.1:2151"}, "")
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
			brs := make([]BlocKRecord, le)
			for i := 0; i < le; i++ {
				sCmd := rdb.LPop(context.Background(), blockInfoKeyInRedis)
				if rCmd.Err() != nil {
					panic("failed to read block records from redis: " + rCmd.Err().Error())
				}
				br := BlocKRecord{}
				_ = json.Unmarshal([]byte(sCmd.Val()), &br)
				brs[i] = br
				fmt.Printf("found a block, ID: %d, addr: %s\n", br.LeaderID, br.LeaderAddr)
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

			for _, v := range brs {
				cli := clis[v.LeaderAddr]
				res, err := cli.AppendToBlock(context.Background(), &segpb.AppendToBlockRequest{
					BlockId: v.LeaderID,
					Events:  &cloudevents.CloudEventBatch{Events: []*cloudevents.CloudEvent{}},
				})
				if err != nil {
					fmt.Printf("failed to append events to block: %s\n", err.Error())
				} else {
					fmt.Printf("success to append events to block: %d\n", res.Offsets)
				}
			}

		},
	}

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

func startMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		log.Error(context.Background(), "Metrics listen and serve failed.", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
