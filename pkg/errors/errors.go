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

package errors

import errpb "github.com/linkall-labs/vanus/proto/pkg/errors"

var (
	// UNKNOWN

	// RESOURCE_NOT_FOUND
	ErrResourceNotFound       = errpb.New("resource not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)
	ErrEventLogNotFound       = errpb.New("eventlog not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)
	ErrSegmentNotFound        = errpb.New("segment not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)
	ErrBlockNotFound          = errpb.New("block not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)
	ErrVolumeInstanceNotFound = errpb.New("volume instance not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)

	// SERVICE_NOT_RUNNING
	ErrServerNotStart            = errpb.New("server not start").WithGRPCCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)
	ErrSegmentServerHasBeenAdded = errpb.New("the segment server has been added").WithGRPCCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)
	ErrServiceState              = errpb.New("service state error").WithGRPCCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)
	ErrWorkerNotStart            = errpb.New("worker not start").WithGRPCCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)

	// SEGMENT_FULL
	ErrSegmentFull           = errpb.New("full").WithGRPCCode(errpb.ErrorCode_SEGMENT_FULL)
	ErrSegmentNotEnoughSpace = errpb.New("not enough space").WithGRPCCode(errpb.ErrorCode_SEGMENT_FULL)

	// INTERNAL
	ErrInternal               = errpb.New("internal error").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrTriggerWorker          = errpb.New("trigger worker error").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrInvalidSegment         = errpb.New("invalid segment").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrInvalidHeartBeat       = errpb.New("invalid heartbeat").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrVolumeInstanceNoServer = errpb.New("no segment server was bound to volume instance").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrJSONMarshal            = errpb.New("json marshal").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrJSONUnMarshal          = errpb.New("json unmarshal").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrAESEncrypt             = errpb.New("aes encrypt").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrAESDecrypt             = errpb.New("aes decrypt").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrUnmarshall             = errpb.New("unmarshall data failed").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrLambdaInvoke           = errpb.New("lambda invoke error").WithGRPCCode(errpb.ErrorCode_INTERNAL)
	ErrLambdaInvokeResponse   = errpb.New("lambda invoke response fail").WithGRPCCode(errpb.ErrorCode_INTERNAL)

	// INVALID_REQUEST
	ErrInvalidRequest          = errpb.New("invalid request").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrCeSQLExpression         = errpb.New("ce sql expression invalid").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrCelExpression           = errpb.New("cel expression invalid").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrFilterAttributeIsEmpty  = errpb.New("filter dialect attribute is empty").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrFilterMultiple          = errpb.New("filter multiple dialects found").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrInvalidHeartBeatRequest = errpb.New("invalid heartbeat request").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrVanusJSONParse          = errpb.New("invalid json").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrTransformInputParse     = errpb.New("transform input invalid").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)

	// RESOURCE_EXIST
	ErrResourceAlreadyExist = errpb.New("resource already exist").WithGRPCCode(errpb.ErrorCode_RESOURCE_EXIST)

	// NOT_LEADER
	ErrNotLeader          = errpb.New("not leader").WithGRPCCode(errpb.ErrorCode_NOT_LEADER)
	ErrNoControllerLeader = errpb.New("no leader controller found").WithGRPCCode(errpb.ErrorCode_NOT_LEADER)
	ErrNotRaftLeader      = errpb.New("the node is not raft leader").WithGRPCCode(errpb.ErrorCode_NOT_LEADER)

	// RESOURCE_EXHAUSTED
	ErrNoAvailableEventLog = errpb.New("no eventlog available").WithGRPCCode(errpb.ErrorCode_RESOURCE_EXHAUSTED)

	// RESOURCE_CAN_NOT_OP
	ErrResourceCanNotOp = errpb.New("resource can not operation").WithGRPCCode(errpb.ErrorCode_RESOURCE_CAN_NOT_OP)
)
