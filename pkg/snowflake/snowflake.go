// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package snowflake

import (
	// standard libraries.
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	// third-party libraries.
	"github.com/sony/sonyflake"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	"github.com/vanus-labs/vanus/api/cluster"
	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/pkg/observability/log"
)

var (
	generator   *snowflake
	once        sync.Once
	fake        bool
	initialized atomic.Bool
)

type snowflake struct {
	snow     *sonyflake.Sonyflake
	client   ctrlpb.SnowflakeControllerClient
	ctrlAddr []string
	n        *node
}

// Initialize refactor in the future.
func Initialize(ctx context.Context, ctrlAddr []string, n *node) error {
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

func Destroy() {
	if generator != nil {
		_, err := generator.client.UnregisterNode(context.Background(),
			&wrapperspb.UInt32Value{Value: uint32(generator.n.logicID())})
		if err != nil {
			log.Warn().Err(err).Msg("failed to unregister snowflake")
		}
	}
}
