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

package event

import (
	// standard libraries.
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	// third-party libraries.
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	segmentpb "github.com/vanus-labs/vanus/api/segment"
)

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event",
		Short: "get events",
		Run:   get,
	}
	cmd.Flags().StringVar(&storeEndpoint, "store", "", "store endpoint")
	cmd.Flags().StringVar(&blockID, "block", "", "block id")
	cmd.Flags().Uint64Var(&offset, "offset", 0, "the start offset")
	cmd.Flags().Uint64Var(&number, "num", 1, "the number of events")
	return cmd
}

func get(cmd *cobra.Command, args []string) {
	id, err := strconv.ParseUint(blockID, 0, 0)
	if err != nil {
		panic(err)
	}

	if number == 0 {
		number = 1
	}

	req := &segmentpb.ReadFromBlockRequest{
		BlockId: id,
		Offset:  int64(offset),
		Number:  int64(number),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.DialContext(ctx, storeEndpoint, opts...)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	cli := segmentpb.NewSegmentServerClient(conn)
	resp, err := cli.ReadFromBlock(ctx, req)
	if err != nil {
		panic(err)
	}

	for _, e := range resp.GetEvents().GetEvents() {
		jsonEvent, err := json.MarshalIndent(e, "", "  ")
		if err != nil {
			panic(err)
		}

		fmt.Println(string(jsonEvent))
	}
}
