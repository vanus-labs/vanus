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

type ErrorCode int32

const (
	// ErrorCode_UNKNOWN 90xx
	ErrorCode_UNKNOWN ErrorCode = 9000

	// ErrorCode_INVALID_REQUEST 91xx
	ErrorCode_INVALID_REQUEST           ErrorCode = 9100
	ErrorCode_INVALID_ARGUMENT          ErrorCode = 9101
	ErrorCode_CESQL_EXPRESSION          ErrorCode = 9102
	ErrorCode_CEL_EXPRESSION            ErrorCode = 9103
	ErrorCode_FILTER_ATTRIBUTE_IS_EMPTY ErrorCode = 9104
	ErrorCode_FILTER_MULTIPLE           ErrorCode = 9105
	ErrorCode_INVALID_HEARTBEAT_REQUEST ErrorCode = 9106
	ErrorCode_JSON_PARSE                ErrorCode = 9107
	ErrorCode_TRANSFORM_INPUT_PARSE     ErrorCode = 9108
	ErrorCode_CORRUPTED_EVENT           ErrorCode = 9109

	// ErrorCode_SERVICE_NOT_RUNNING 92xx
	ErrorCode_SERVICE_NOT_RUNNING           ErrorCode = 9200
	ErrorCode_SEGMENT_SERVER_HAS_BEEN_ADDED ErrorCode = 9201
	ErrorCode_SERVICE_STATE_ERROR           ErrorCode = 9202
	ErrorCode_WORKER_NOT_RUNNING            ErrorCode = 9203

	// ErrorCode_RESOURCE_EXIST 93xx
	ErrorCode_RESOURCE_EXIST ErrorCode = 9300

	// ErrorCode_RESOURCE_NOT_FOUND 94xx
	ErrorCode_RESOURCE_NOT_FOUND ErrorCode = 9400
	ErrorCode_EVENTBUS_NOT_FOUND ErrorCode = 9401
	ErrorCode_EVENTLOG_NOT_FOUND ErrorCode = 9402
	ErrorCode_SEGMENT_NOT_FOUND  ErrorCode = 9403
	ErrorCode_BLOCK_NOT_FOUND    ErrorCode = 9404
	ErrorCode_VOLUME_NOT_FOUND   ErrorCode = 9405

	// ErrorCode_INTERNAL 95xx
	ErrorCode_INTERNAL               ErrorCode = 9500
	ErrorCode_TRIGGER_WORKER         ErrorCode = 9501
	ErrorCode_INVALID_SEGMENT        ErrorCode = 9502
	ErrorCode_INVAILD_HEAETBEAT      ErrorCode = 9503
	ErrorCode_VOLUME_NO_SERVER       ErrorCode = 9504
	ErrorCode_JSON_MARSHAL           ErrorCode = 9505
	ErrorCode_JSON_UNMARSHAL         ErrorCode = 9506
	ErrorCode_AES_ENCRYPT            ErrorCode = 9507
	ErrorCode_AES_DECRYPT            ErrorCode = 9508
	ErrorCode_UNMARSHAL              ErrorCode = 9509
	ErrorCode_LAMBDA_INVOKE          ErrorCode = 9510
	ErrorCode_LAMBDA_INVOKE_RESPONSE ErrorCode = 9511

	// ErrorCode_SEGMENT_FULL 96xx
	ErrorCode_SEGMENT_FULL            ErrorCode = 9600
	ErrorCode_SEGMENT_NO_ENOUGH_SPACE ErrorCode = 9601
	ErrorCode_OFFSET_UNDERFLOW        ErrorCode = 9602
	ErrorCode_OFFSET_OVERFLOW         ErrorCode = 9603
	ErrorCode_OFFSET_ON_END           ErrorCode = 9604
	ErrorCode_BLOCK_NOT_SUPPORTED     ErrorCode = 9605
	ErrorCode_NOT_WRITABLE            ErrorCode = 9606
	ErrorCode_NOT_READABLE            ErrorCode = 9607
	ErrorCode_TRY_AGAIN               ErrorCode = 9608
	ErrorCode_NO_ENDPOINT             ErrorCode = 9609
	ErrorCode_CLOSED                  ErrorCode = 9610

	// ErrorCode_NOT_LEADER 97xx
	ErrorCode_NOT_LEADER           ErrorCode = 9700
	ErrorCode_NO_CONTROLLER_LEADER ErrorCode = 9701
	ErrorCode_NOT_RAFT_LEADER      ErrorCode = 9702
	ErrorCodeNotReady              ErrorCode = 9704

	// ErrorCode_RESERVE 98xx

	// ErrorCode_OTHERS 99xx
	ErrorCode_RESOURCE_EXHAUSTED  ErrorCode = 9901
	ErrorCode_RESOURCE_CAN_NOT_OP ErrorCode = 9902
)

var (
	// UNKNOWN
	ErrUnknown = New("unknown").WithGRPCCode(ErrorCode_UNKNOWN)

	// RESOURCE_NOT_FOUND
	ErrResourceNotFound       = New("resource not found").WithGRPCCode(ErrorCode_RESOURCE_NOT_FOUND)
	ErrEventlogNotFound       = New("eventlog not found").WithGRPCCode(ErrorCode_EVENTLOG_NOT_FOUND)
	ErrSegmentNotFound        = New("segment not found").WithGRPCCode(ErrorCode_SEGMENT_NOT_FOUND)
	ErrBlockNotFound          = New("block not found").WithGRPCCode(ErrorCode_BLOCK_NOT_FOUND)
	ErrVolumeInstanceNotFound = New("volume instance not found").WithGRPCCode(ErrorCode_VOLUME_NOT_FOUND)

	// SERVICE_NOT_RUNNING
	ErrServerNotStart            = New("server not start").WithGRPCCode(ErrorCode_SERVICE_NOT_RUNNING)
	ErrSegmentServerHasBeenAdded = New("the segment server has been added").WithGRPCCode(
		ErrorCode_SEGMENT_SERVER_HAS_BEEN_ADDED)
	ErrServiceState   = New("service state error").WithGRPCCode(ErrorCode_SERVICE_STATE_ERROR)
	ErrWorkerNotStart = New("worker not start").WithGRPCCode(ErrorCode_WORKER_NOT_RUNNING)

	// SEGMENT_FULL
	ErrSegmentFull           = New("segment full").WithGRPCCode(ErrorCode_SEGMENT_FULL)
	ErrSegmentNotEnoughSpace = New("not enough space").WithGRPCCode(ErrorCode_SEGMENT_NO_ENOUGH_SPACE)
	ErrOffsetUnderflow       = New("the offset underflow").WithGRPCCode(ErrorCode_OFFSET_UNDERFLOW)
	ErrOffsetOverflow        = New("the offset overflow").WithGRPCCode(ErrorCode_OFFSET_OVERFLOW)
	ErrOffsetOnEnd           = New("the offset on end").WithGRPCCode(ErrorCode_OFFSET_ON_END)
	ErrBlockNotSupported     = New("block not supported").WithGRPCCode(ErrorCode_BLOCK_NOT_SUPPORTED)
	ErrNotWritable           = New("not writable").WithGRPCCode(ErrorCode_NOT_WRITABLE)
	ErrNotReadable           = New("not readable").WithGRPCCode(ErrorCode_NOT_READABLE)
	ErrTryAgain              = New("try again").WithGRPCCode(ErrorCode_TRY_AGAIN)
	ErrNoEndpoint            = New("no endpoint").WithGRPCCode(ErrorCode_NO_ENDPOINT)
	ErrClosed                = New("closed").WithGRPCCode(ErrorCode_CLOSED)

	// INTERNAL
	ErrInternal               = New("internal error").WithGRPCCode(ErrorCode_INTERNAL)
	ErrTriggerWorker          = New("trigger worker error").WithGRPCCode(ErrorCode_TRIGGER_WORKER)
	ErrInvalidSegment         = New("invalid segment").WithGRPCCode(ErrorCode_INVALID_SEGMENT)
	ErrInvalidHeartBeat       = New("invalid heartbeat").WithGRPCCode(ErrorCode_INVAILD_HEAETBEAT)
	ErrVolumeInstanceNoServer = New("no segment server was bound to volume instance").WithGRPCCode(ErrorCode_VOLUME_NO_SERVER)
	ErrJSONMarshal            = New("json marshal").WithGRPCCode(ErrorCode_JSON_MARSHAL)
	ErrJSONUnMarshal          = New("json unmarshal").WithGRPCCode(ErrorCode_JSON_UNMARSHAL)
	ErrAESEncrypt             = New("aes encrypt").WithGRPCCode(ErrorCode_AES_ENCRYPT)
	ErrAESDecrypt             = New("aes decrypt").WithGRPCCode(ErrorCode_AES_DECRYPT)
	ErrUnmarshall             = New("unmarshall data failed").WithGRPCCode(ErrorCode_UNMARSHAL)
	ErrLambdaInvoke           = New("lambda invoke error").WithGRPCCode(ErrorCode_LAMBDA_INVOKE)
	ErrLambdaInvokeResponse   = New("lambda invoke response fail").WithGRPCCode(ErrorCode_LAMBDA_INVOKE_RESPONSE)

	// INVALID_REQUEST
	ErrInvalidRequest          = New("invalid request").WithGRPCCode(ErrorCode_INVALID_REQUEST)
	ErrInvalidArgument         = New("invalid argument").WithGRPCCode(ErrorCode_INVALID_ARGUMENT)
	ErrCeSQLExpression         = New("ce sql expression invalid").WithGRPCCode(ErrorCode_CESQL_EXPRESSION)
	ErrCelExpression           = New("cel expression invalid").WithGRPCCode(ErrorCode_CEL_EXPRESSION)
	ErrFilterAttributeIsEmpty  = New("filter dialect attribute is empty").WithGRPCCode(ErrorCode_FILTER_ATTRIBUTE_IS_EMPTY)
	ErrFilterMultiple          = New("filter multiple dialects found").WithGRPCCode(ErrorCode_FILTER_MULTIPLE)
	ErrInvalidHeartBeatRequest = New("invalid heartbeat request").WithGRPCCode(ErrorCode_INVALID_HEARTBEAT_REQUEST)
	ErrVanusJSONParse          = New("invalid json").WithGRPCCode(ErrorCode_JSON_PARSE)
	ErrTransformInputParse     = New("transform input invalid").WithGRPCCode(ErrorCode_TRANSFORM_INPUT_PARSE)
	ErrCorruptedEvent          = New("corrupted event").WithGRPCCode(ErrorCode_CORRUPTED_EVENT)

	// ErrResourceAlreadyExist
	ErrResourceAlreadyExist = New("resource already exist").WithGRPCCode(ErrorCode_RESOURCE_EXIST)

	// ErrNotLeader not leader
	ErrNotLeader          = New("not leader").WithGRPCCode(ErrorCode_NOT_LEADER)
	ErrNotReady           = New("not ready").WithGRPCCode(ErrorCodeNotReady)
	ErrNoControllerLeader = New("no leader controller found").WithGRPCCode(ErrorCode_NO_CONTROLLER_LEADER)
	ErrNotRaftLeader      = New("the node is not raft leader").WithGRPCCode(ErrorCode_NOT_RAFT_LEADER)

	// RESOURCE_EXHAUSTED
	ErrNoAvailableEventlog = New("no eventlog available").WithGRPCCode(ErrorCode_RESOURCE_EXHAUSTED)

	// NO_MORE_MESSAGE

	// RESOURCE_CAN_NOT_OP
	ErrResourceCanNotOp = New("resource can not operation").WithGRPCCode(ErrorCode_RESOURCE_CAN_NOT_OP)
)
