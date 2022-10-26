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

package vanus

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/controller"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/sony/sonyflake"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"strconv"
	"sync"
	"time"
)

type ID uint64

var (
	emptyID = ID(0)
	lock    = sync.Mutex{}
	base    = 10
	bitSize = 64
)

func EmptyID() ID {
	return emptyID
}

var (
	generator *snowflake
	once      sync.Once
)

type snowflake struct {
	snow     *sonyflake.Sonyflake
	client   ctrlpb.SnowflakeControllerClient
	ctrlAddr []string
	nodeID   uint16
}

func InitSnowflake(ctrlAddr []string, nodeID uint16) error {
	var err error
	once.Do(func() {
		snow := &snowflake{
			client:   controller.NewSnowflakeController(ctrlAddr, insecure.NewCredentials()),
			ctrlAddr: ctrlAddr,
			nodeID:   nodeID,
		}
		sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: time.Time{},
			MachineID: func() (uint16, error) {
				return nodeID, nil
			},
			CheckMachineID: nil,
		})
		var startTime *timestamppb.Timestamp
		startTime, err = snow.client.GetClusterStartTime(context.Background(), &empty.Empty{})
		if err != nil {
			return
		}
		snow.snow = sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: startTime.AsTime(),
			MachineID: func() (uint16, error) {
				return nodeID, nil
			},
			CheckMachineID: func(u uint16) bool {
				val, err := snow.client.CheckNodeID(context.Background(), &wrapperspb.UInt32Value{Value: uint32(nodeID)})
				if err != nil {
					log.Warning(nil, "check node ID to controller failed", map[string]interface{}{
						log.KeyError: err,
					})
					return false
				}
				return val.Value
			},
		})
	})
	return err
}

func NewID() (ID, error) {
	lock.Lock()
	defer lock.Unlock()

	id, err := generator.snow.NextID()
	if err != nil {
		return EmptyID(), err
	}
	return ID(id), nil
}

func NewIDFromUint64(id uint64) ID {
	return ID(id)
}

func NewIDFromString(id string) (ID, error) {
	i, err := strconv.ParseUint(id, base, bitSize)
	if err != nil {
		return emptyID, err
	}
	return ID(i), nil
}

func (id ID) String() string {
	return fmt.Sprintf("%X", uint64(id))
}

func (id ID) Uint64() uint64 {
	return uint64(id)
}

func (id ID) Key() string {
	return id.String()
}

func (id ID) Equals(cID ID) bool {
	return id.Uint64() == cID.Uint64()
}
