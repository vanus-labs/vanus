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

//go:generate mockgen -source=controller.go -destination=mock_controller.go -package=cluster
package cluster

import (
	"context"
	"errors"
	errors2 "github.com/vanus-labs/vanus/pkg/errors"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
)

var defaultClusterStartTimeout = 3 * time.Minute

type Topology struct {
	ControllerLeader string
	ControllerURLs   []string
	Uptime           time.Time
}

type Cluster interface {
	WaitForControllerReady(createEventbus bool) error
	Status() Topology
	IsReady(createEventbus bool) bool
	NamespaceService() NamespaceService
	EventbusService() EventbusService
	SegmentService() SegmentService
	EventlogService() EventlogService
	TriggerService() TriggerService
	IDService() IDService
}

type NamespaceService interface {
	RawClient() ctrlpb.NamespaceControllerClient
	GetSystemNamespace(ctx context.Context) (*meta.Namespace, error)
	GetDefaultNamespace(ctx context.Context) (*meta.Namespace, error)
	GetNamespace(ctx context.Context, id uint64) (*meta.Namespace, error)
}

type EventbusService interface {
	IsExist(ctx context.Context, id uint64) bool
	CreateSystemEventbusIfNotExist(ctx context.Context, name string, desc string) (*meta.Eventbus, error)
	Delete(ctx context.Context, id uint64) error
	GetSystemEventbusByName(ctx context.Context, name string) (*meta.Eventbus, error)
	GetEventbus(ctx context.Context, id uint64) (*meta.Eventbus, error)
	RawClient() ctrlpb.EventbusControllerClient
}

type EventlogService interface {
	RawClient() ctrlpb.EventlogControllerClient
}

type TriggerService interface {
	RawClient() ctrlpb.TriggerControllerClient
	RegisterHeartbeat(ctx context.Context, interval time.Duration, reqFunc func() interface{}) error
}

type IDService interface {
	RawClient() ctrlpb.SnowflakeControllerClient
}

type SegmentService interface {
	RegisterHeartbeat(ctx context.Context, interval time.Duration, reqFunc func() interface{}) error
	RawClient() ctrlpb.SegmentControllerClient
}

func NewClusterController(endpoints []string, credentials credentials.TransportCredentials) Cluster {

	// single instance
	cc := raw_client.NewConnection(endpoints, credentials)
	c := &cluster{
		cc:                cc,
		nsSvc:             newNamespaceService(cc),
		segmentSvc:        newSegmentService(cc),
		elSvc:             newEventlogService(cc),
		triggerSvc:        newTriggerService(cc),
		idSvc:             newIDService(cc),
		ping:              raw_client.NewPingClient(cc),
		controllerAddress: endpoints,
	}
	c.ebSvc = newEventbusService(cc, c.NamespaceService())
	return c
}

type cluster struct {
	controllerAddress []string
	cc                *raw_client.Conn
	nsSvc             NamespaceService
	ebSvc             EventbusService
	elSvc             EventlogService
	triggerSvc        TriggerService
	idSvc             IDService
	segmentSvc        SegmentService
	ping              ctrlpb.PingServerClient
}

func (c *cluster) WaitForControllerReady(createEventbus bool) error {
	start := time.Now()
	log.Info(context.Background(), "wait for controller is ready", nil)
	t := time.NewTicker(defaultClusterStartTimeout)
	defer t.Stop()
	for !c.IsReady(createEventbus) {
		select {
		case <-t.C:
			return errors.New("cluster isn't ready")
		default:
			time.Sleep(time.Second)
		}
	}

	log.Info(context.Background(), "controller is ready", map[string]interface{}{
		"waiting_time": time.Now().Sub(start),
	})
	return nil
}

func (c *cluster) IsReady(createEventbus bool) bool {
	res, err := c.ping.Ping(context.Background(), &emptypb.Empty{})
	if err != nil {
		if !errors.Is(err, errors2.ErrNotLeader) {
			log.Warning(context.Background(), "failed to ping controller", map[string]interface{}{
				log.KeyError: err,
			})
		}
		return false
	}
	if res.LeaderAddr == "" {
		return false
	}
	return !createEventbus || (createEventbus && res.GetIsEventbusReady())
}

func (c *cluster) Status() Topology {
	// TODO(wenfeng)
	return Topology{}
}

func (c *cluster) NamespaceService() NamespaceService {
	return c.nsSvc
}

func (c *cluster) EventbusService() EventbusService {
	return c.ebSvc
}

func (c *cluster) SegmentService() SegmentService {
	return c.segmentSvc
}

func (c *cluster) EventlogService() EventlogService {
	return c.elSvc
}

func (c *cluster) TriggerService() TriggerService {
	return c.triggerSvc
}

func (c *cluster) IDService() IDService {
	return c.idSvc
}
