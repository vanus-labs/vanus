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
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	cepb "github.com/vanus-labs/vanus/api/cloudevents"
	segmentpb "github.com/vanus-labs/vanus/api/segment"
)

func CreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event",
		Short: "create event",
		Run:   create,
	}
	cmd.Flags().StringVar(&storeEndpoint, "store", "", "store endpoint")
	cmd.Flags().StringVar(&blockID, "block", "", "block id")
	cmd.Flags().StringVar(&eventID, "id", "", "event id")
	cmd.Flags().StringVar(&eventSource, "source", "https://github.com/vanus-labs/vanus/tool/vsrepair", "event source")
	cmd.Flags().StringVar(&eventType, "type", "ai.vanus.core.vsrepair.event.create", "event type")
	cmd.Flags().StringVar(&eventData, "data", "", "event data")
	return cmd
}

func create(_ *cobra.Command, _ []string) {
	id, err := strconv.ParseUint(blockID, 0, 0)
	if err != nil {
		panic(err)
	}

	event := &cepb.CloudEvent{
		SpecVersion: "1.0",
		Source:      eventSource,
		Type:        eventType,
	}
	if eventID != "" {
		event.Id = eventID
	} else {
		event.Id = uuid.NewString()
	}
	if eventData != "" {
		event.Data = &cepb.CloudEvent_TextData{
			TextData: eventData,
		}
	}

	req := &segmentpb.AppendToBlockRequest{
		BlockId: id,
		Events: &cepb.CloudEventBatch{
			Events: []*cepb.CloudEvent{event},
		},
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
	resp, err := cli.AppendToBlock(ctx, req)
	if err != nil {
		panic(err)
	}

	jsonOffsets, err := json.MarshalIndent(resp.GetOffsets(), "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonOffsets))
}
