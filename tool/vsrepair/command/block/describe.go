// Copyright 2023 Linkall Inc.
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

package block

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
	metapb "github.com/vanus-labs/vanus/api/meta"
	segmentpb "github.com/vanus-labs/vanus/api/segment"

	// this project.
	"github.com/vanus-labs/vanus/server/store/vsb"
	"github.com/vanus-labs/vanus/tool/vsrepair/block"
	"github.com/vanus-labs/vanus/tool/vsrepair/meta"
)

const (
	defaultTimeout = 10 * time.Second
)

func DescribeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "block ID",
		Short: "Show details of a specific Block.",
		Args:  cobra.ExactArgs(1),
		RunE:  describe,
	}
	cmd.Flags().StringVar(&volumePath, "volume", "", "volume path")
	cmd.Flags().StringVar(&storeEndpoint, "store", "", "store endpoint")
	cmd.Flags().BoolVar(&noVSB, "no-vsb", false, "not describe vsb detail")
	cmd.Flags().BoolVar(&noRaft, "no-raft", false, "not describe raft detail")
	cmd.Flags().BoolVar(&withStatus, "with-status", false, "describe block status")
	if err := cobra.MarkFlagDirname(cmd.Flags(), "volume"); err != nil {
		panic(err)
	}
	return cmd
}

type blockDetail struct {
	ID     uint64                    `json:"ID"`
	VSB    *vsb.Header               `json:"VSB,omitempty"`
	Raft   *meta.RaftDetail          `json:"Raft,omitempty"`
	Status *metapb.SegmentHealthInfo `json:"Status,omitempty"`
}

func describe(cmd *cobra.Command, args []string) error {
	id, err := strconv.ParseUint(args[0], 0, 64)
	if err != nil {
		return err
	}

	if (!noVSB || !noRaft) && volumePath == "" {
		return fmt.Errorf("volume path is empty")
	}
	if withStatus && storeEndpoint == "" {
		return fmt.Errorf("store endpoint is empty")
	}

	d := blockDetail{
		ID: id,
	}

	if !noVSB {
		vsbDetail, err2 := block.VSBDetail(volumePath, id)
		if err2 != nil {
			return err2
		}
		d.VSB = &vsbDetail
	}

	if !noRaft {
		db, err2 := meta.Open(volumePath, meta.ReadOnly())
		if err2 != nil {
			return err2
		}
		defer db.Close()

		raftDetail, err2 := db.GetRaftDetail(id)
		if err2 == nil {
			d.Raft = &raftDetail
		} else if err2 != meta.ErrNotFound { //nolint:errorlint // compare to meta.ErrNotFound is ok.
			return err2
		}
	}

	if withStatus {
		req := &segmentpb.DescribeBlockRequest{
			Id: id,
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
		defer cancel()

		opts := []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		conn, err2 := grpc.DialContext(ctx, storeEndpoint, opts...)
		if err2 != nil {
			return err2
		}
		defer func() {
			_ = conn.Close()
		}()

		cli := segmentpb.NewSegmentServerClient(conn)
		resp, err2 := cli.DescribeBlock(ctx, req)
		if err2 != nil {
			return err2
		}

		d.Status = resp.GetInfo()
	}

	jsonDetail, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(jsonDetail))

	return nil
}
