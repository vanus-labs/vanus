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
	// standard libraries.
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	// third-party libraries.
	"github.com/sony/sonyflake"
	"go.uber.org/atomic"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/primitive"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
)

var ErrEmptyID = primitive.ErrEmptyID

type ID = primitive.ID

func EmptyID() ID {
	return primitive.EmptyID()
}

func NewIDFromUint64(id uint64) ID {
	return primitive.NewIDFromUint64(id)
}

func NewIDFromString(id string) (ID, error) {
	return primitive.NewIDFromString(id)
}

type node struct {
	start uint16
	end   uint16
	id    uint16
	svc   Service
}

type Service int

const (
	ControllerService Service = iota
	StoreService
	UnknownService

	// NodeName space: [0, 65535], DON'T CHANGE THEM!!!
	controllerNodeIDStart           = uint16(16)
	reservedControlPanelNodeIDStart = uint16(32)
	storeNodeIDStart                = uint16(1024)
	reservedNodeIDStart             = uint16(8192)

	waitFinishInitSpinInterval = 50 * time.Millisecond
)

func (s Service) Name() string {
	switch s {
	case ControllerService:
		return "ControllerService"
	case StoreService:
		return "StoreService"
	default:
		return "UnknownService"
	}
}

func NewNode(svc Service, id uint16) *node { //nolint: revive // it's ok
	switch svc {
	case ControllerService:
		return &node{
			start: controllerNodeIDStart,
			end:   reservedControlPanelNodeIDStart,
			svc:   svc,
			id:    id,
		}
	case StoreService:
		return &node{
			start: storeNodeIDStart,
			end:   reservedNodeIDStart,
			svc:   svc,
			id:    id,
		}
	}
	return &node{
		start: reservedNodeIDStart,
		end:   reservedNodeIDStart,
		svc:   UnknownService,
		id:    id,
	}
}

func (n *node) logicID() uint16 {
	return n.start + n.id
}

func (n *node) valid() bool {
	return n.logicID() < n.end && n.logicID() >= n.start
}

var (
	generator   *snowflake
	once        sync.Once
	fake        bool
	initialized atomic.Bool

	lock = sync.Mutex{}
)

type snowflake struct {
	snow     *sonyflake.Sonyflake
	client   ctrlpb.SnowflakeControllerClient
	ctrlAddr []string
	n        *node
}

// InitFakeSnowflake just only used for Uint Test.
func InitFakeSnowflake() {
	fake = true
}

// InitSnowflake refactor in the future.
func InitSnowflake(ctx context.Context, ctrlAddr []string, n *node) error {
	if !n.valid() {
		return fmt.Errorf("the nodeID number: %d exceeded, range of %s is [%d, %d)",
			n.logicID(), n.svc.Name(), n.start, n.end)
	}
	initService := func() error {
		ctrl := cluster.NewClusterController(ctrlAddr, insecure.NewCredentials())
		snow := &snowflake{
			client:   ctrl.IDService().RawClient(),
			ctrlAddr: ctrlAddr,
			n:        n,
		}
		var startTime *timestamppb.Timestamp
		startTime, err := snow.client.GetClusterStartTime(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}

		snow.snow = sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: startTime.AsTime(),
			MachineID: func() (uint16, error) {
				return n.logicID(), nil
			},
			CheckMachineID: func(u uint16) bool {
				_, err := snow.client.RegisterNode(ctx, &wrapperspb.UInt32Value{
					Value: uint32(u),
				})
				if err != nil {
					log.Error(ctx).Err(err).Msg("register snowflake failed")
					return false
				}
				return true
			},
		})
		if snow.snow == nil {
			return fmt.Errorf("init snowflake failed")
		}
		generator = snow
		initialized.Store(true)
		log.Info(ctx).Uint16("node_id", snow.n.logicID()).Msg("succeed to init VolumeID generator")
		return nil
	}
	var err error
	once.Do(func() {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		ticker := time.NewTicker(time.Second)

		for {
			select {
			case <-ctx.Done():
				err = errors.New("init snowflake ID service timeout")
				return
			case <-ticker.C:
				err = initService()
				if err == nil {
					return
				}
			}
		}
	})
	return err
}

func DestroySnowflake() {
	if generator != nil {
		_, err := generator.client.UnregisterNode(context.Background(),
			&wrapperspb.UInt32Value{Value: uint32(generator.n.logicID())})
		if err != nil {
			log.Warn().Err(err).Msg("failed to unregister snowflake")
		}
	}
}

func NewID() (ID, error) {
	if fake {
		return NewTestID(), nil
	}

	for !initialized.Load() {
		time.Sleep(waitFinishInitSpinInterval)
	}

	id, err := generator.snow.NextID()
	if err != nil {
		return EmptyID(), err
	}
	return ID(id), nil
}

// NewTestID only used for Uint Test.
func NewTestID() ID {
	lock.Lock()
	defer lock.Unlock()

	// avoiding same id
	time.Sleep(time.Microsecond)
	return ID(time.Now().UnixNano())
}

type IDList []ID

func (l IDList) Contains(id ID) bool {
	for _, _id := range l {
		if _id == id {
			return true
		}
	}
	return false
}
