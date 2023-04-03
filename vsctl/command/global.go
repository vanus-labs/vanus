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

package command

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/grpc_credentials"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

const (
	FormatJSON = "json"
)

const (
	RespCodeOK          int32 = 200
	DefaultOperatorPort       = 8089
	HttpPrefix                = "http://"
	BaseUrl                   = "/api/v1"
)

// Annotations supported by Core
const (
	CoreComponentImagePullPolicyAnnotation = "core.vanus.ai/image-pull-policy"
	// Etcd
	CoreComponentEtcdPortClientAnnotation        = "core.vanus.ai/etcd-port-client"
	CoreComponentEtcdPortPeerAnnotation          = "core.vanus.ai/etcd-port-peer"
	CoreComponentEtcdReplicasAnnotation          = "core.vanus.ai/etcd-replicas"
	CoreComponentEtcdStorageSizeAnnotation       = "core.vanus.ai/etcd-storage-size"
	CoreComponentEtcdStorageClassAnnotation      = "core.vanus.ai/etcd-storage-class"
	CoreComponentEtcdResourceLimitsCpuAnnotation = "core.vanus.ai/etcd-resource-limits-cpu"
	CoreComponentEtcdResourceLimitsMemAnnotation = "core.vanus.ai/etcd-resource-limits-mem"
	// Controller
	CoreComponentControllerSvcPortAnnotation           = "core.vanus.ai/controller-service-port"
	CoreComponentControllerReplicasAnnotation          = "core.vanus.ai/controller-replicas"
	CoreComponentControllerSegmentCapacityAnnotation   = "core.vanus.ai/controller-segment-capacity"
	CoreComponentControllerResourceLimitsCpuAnnotation = "core.vanus.ai/controller-resource-limits-cpu"
	CoreComponentControllerResourceLimitsMemAnnotation = "core.vanus.ai/controller-resource-limits-mem"
	// Root Controller
	CoreComponentRootControllerSvcPortAnnotation = "core.vanus.ai/root-controller-service-port"
	// Store
	CoreComponentStoreReplicasAnnotation          = "core.vanus.ai/store-replicas"
	CoreComponentStoreStorageSizeAnnotation       = "core.vanus.ai/store-storage-size"
	CoreComponentStoreStorageClassAnnotation      = "core.vanus.ai/store-storage-class"
	CoreComponentStoreResourceLimitsCpuAnnotation = "core.vanus.ai/store-resource-limits-cpu"
	CoreComponentStoreResourceLimitsMemAnnotation = "core.vanus.ai/store-resource-limits-mem"
	// Gateway
	CoreComponentGatewayPortProxyAnnotation           = "core.vanus.ai/gateway-port-proxy"
	CoreComponentGatewayPortCloudEventsAnnotation     = "core.vanus.ai/gateway-port-cloudevents"
	CoreComponentGatewayNodePortProxyAnnotation       = "core.vanus.ai/gateway-nodeport-proxy"
	CoreComponentGatewayNodePortCloudEventsAnnotation = "core.vanus.ai/gateway-nodeport-cloudevents"
	CoreComponentGatewayReplicasAnnotation            = "core.vanus.ai/gateway-replicas"
	CoreComponentGatewayResourceLimitsCpuAnnotation   = "core.vanus.ai/gateway-resource-limits-cpu"
	CoreComponentGatewayResourceLimitsMemAnnotation   = "core.vanus.ai/gateway-resource-limits-mem"
	// Trigger
	CoreComponentTriggerReplicasAnnotation          = "core.vanus.ai/trigger-replicas"
	CoreComponentTriggerResourceLimitsCpuAnnotation = "core.vanus.ai/trigger-resource-limits-cpu"
	CoreComponentTriggerResourceLimitsMemAnnotation = "core.vanus.ai/trigger-resource-limits-mem"
	// Timer
	CoreComponentTimerReplicasAnnotation          = "core.vanus.ai/timer-replicas"
	CoreComponentTimerTimingWheelTickAnnotation   = "core.vanus.ai/timer-timingwheel-tick"
	CoreComponentTimerTimingWheelSizeAnnotation   = "core.vanus.ai/timer-timingwheel-size"
	CoreComponentTimerTimingWheelLayersAnnotation = "core.vanus.ai/timer-timingwheel-layers"
	CoreComponentTimerResourceLimitsCpuAnnotation = "core.vanus.ai/timer-resource-limits-cpu"
	CoreComponentTimerResourceLimitsMemAnnotation = "core.vanus.ai/timer-resource-limits-mem"
)

// Annotations supported by Connector
const (
	ConnectorDeploymentReplicasAnnotation = "connector.vanus.ai/deployment-replicas"
	ConnectorServiceTypeAnnotation        = "connector.vanus.ai/service-type"
	ConnectorServicePortAnnotation        = "connector.vanus.ai/service-port"
	ConnectorNetworkHostDomainAnnotation  = "connector.vanus.ai/network-host-domain"
)

var retryTime = 30

type GlobalFlags struct {
	Endpoint         string
	OperatorEndpoint string
	Debug            bool
	ConfigFile       string
	Format           string
	Token            string
}

var (
	client proxypb.ControllerProxyClient
	cc     *grpc.ClientConn
)

func InitGatewayClient(cmd *cobra.Command) {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	token, err := cmd.Flags().GetString("token")
	if err != nil {
		cmdFailedf(cmd, "get token failed: %s", err)
	}
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(grpc_credentials.NewVanusPerRPCCredentials(token)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		panic("failed to dial gateway: " + err.Error())
	}
	cc = conn
	client = proxypb.NewControllerProxyClient(conn)
}

func DestroyGatewayClient() {
	if cc != nil {
		if err := cc.Close(); err != nil {
			color.Yellow(fmt.Sprintf("close grpc connection error: %s", err.Error()))
		}
	}
}

func mustGetGatewayCloudEventsEndpoint(cmd *cobra.Command) string {
	//res, err := client.ClusterInfo(context.Background(), &emptypb.Empty{})
	//if err != nil {
	//	cmdFailedf(cmd, "get cloudevents endpoint failed: %s", err)
	//}
	sp := strings.Split(mustGetGatewayEndpoint(cmd), ":")
	v, _ := strconv.ParseInt(sp[1], 10, 64)
	return fmt.Sprintf("%s:%d", sp[0], v+1)
}

func mustGetGatewayEndpoint(cmd *cobra.Command) string {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	return endpoint
}

func IsFormatJSON(cmd *cobra.Command) bool {
	v, err := cmd.Flags().GetString("format")
	if err != nil {
		return false
	}
	return strings.ToLower(v) == FormatJSON
}

func mustGetEventbusID(namespace, name string) vanus.ID {
	if namespace == "" {
		namespace = "default"
		color.Green("the namespace not specified, using [default] namespace")
	}
	eb, err := client.GetEventbusWithHumanFriendly(context.Background(),
		&ctrlpb.GetEventbusWithHumanFriendlyRequest{
			NamespaceId:  mustGetNamespaceID(namespace).Uint64(),
			EventbusName: name,
		})
	if err != nil {
		color.Red("failed to query eventbus id: %s", Error(err))
		os.Exit(1)
	}
	return vanus.NewIDFromUint64(eb.Id)
}

func mustGetNamespaceID(namespace string) vanus.ID {
	eb, err := client.GetNamespaceWithHumanFriendly(context.Background(), wrapperspb.String(namespace))
	if err != nil {
		color.Red("failed to query namespace id: %s", Error(err))
		os.Exit(1)
	}
	return vanus.NewIDFromUint64(eb.Id)
}
