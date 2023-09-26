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
	// ErrorCodeUnknown 90xx.
	ErrorCodeUnknown ErrorCode = 9000

	// ErrorCodeInvalidRequest 91xx.
	ErrorCodeInvalidRequest          ErrorCode = 9100
	ErrorCodeInvalidArgument         ErrorCode = 9101
	ErrorCodeCESQLExpression         ErrorCode = 9102
	ErrorCodeCELExpression           ErrorCode = 9103
	ErrorCodeFilterAttributeIsEmpty  ErrorCode = 9104
	ErrorCodeFilterMultiple          ErrorCode = 9105
	ErrorCodeInvalidHeartbeatRequest ErrorCode = 9106
	ErrorCodeJSONParse               ErrorCode = 9107
	ErrorCodeTransformInputParse     ErrorCode = 9108
	ErrorCodeCorruptedEvent          ErrorCode = 9109

	// ErrorCodeServiceNotRunning 92xx.
	ErrorCodeServiceNotRunning         ErrorCode = 9200
	ErrorCodeSegmentServerHasBeenAdded ErrorCode = 9201
	ErrorCodeServiceStateError         ErrorCode = 9202
	ErrorCodeWorkerNotRunning          ErrorCode = 9203

	// ErrorCodeResourceExist 93xx
	ErrorCodeResourceExist ErrorCode = 9300

	// ErrorCodeResourceNotFound 94xx.
	ErrorCodeResourceNotFound ErrorCode = 9400
	ErrorCodeEventbusNotFound ErrorCode = 9401
	ErrorCodeEventlogNotFound ErrorCode = 9402
	ErrorCodeSegmentNotFound  ErrorCode = 9403
	ErrorCodeBlockNotFound    ErrorCode = 9404
	ErrorCodeVolumeNotFound   ErrorCode = 9405

	// ErrorCodeInternal 95xx.
	ErrorCodeInternal             ErrorCode = 9500
	ErrorCodeTriggerWorker        ErrorCode = 9501
	ErrorCodeInvalidSegment       ErrorCode = 9502
	ErrorCodeInvalidHeartbeat     ErrorCode = 9503
	ErrorCodeVolumeNoServer       ErrorCode = 9504
	ErrorCodeJSONMarshal          ErrorCode = 9505
	ErrorCodeJSONUnmarshal        ErrorCode = 9506
	ErrorCodeAESEncrypt           ErrorCode = 9507
	ErrorCodeAESDecrypt           ErrorCode = 9508
	ErrorCodeUnmarshal            ErrorCode = 9509
	ErrorCodeLambdaInvoke         ErrorCode = 9510
	ErrorCodeLambdaInvokeResponse ErrorCode = 9511
	ErrorCodeInvalidJSONPath      ErrorCode = 9512
	ErrorCodeJSONPathNotExist     ErrorCode = 9513

	// ErrorCodeSegmentFull 96xx.
	ErrorCodeSegmentFull          ErrorCode = 9600
	ErrorCodeSegmentNoEnoughSpace ErrorCode = 9601
	ErrorCodeOffsetUnderflow      ErrorCode = 9602
	ErrorCodeOffsetOverflow       ErrorCode = 9603
	ErrorCodeOffsetOnEnd          ErrorCode = 9604
	ErrorCodeBlockNotSupported    ErrorCode = 9605
	ErrorCodeNotWritable          ErrorCode = 9606
	ErrorCodeNotReadable          ErrorCode = 9607
	ErrorCodeTryAgain             ErrorCode = 9608
	ErrorCodeNoEndpoint           ErrorCode = 9609
	ErrorCodeClosed               ErrorCode = 9610

	// ErrorCodeNotLeader 97xx.
	ErrorCodeNotLeader          ErrorCode = 9700
	ErrorCodeNoControllerLeader ErrorCode = 9701
	ErrorCodeNotRaftLeader      ErrorCode = 9702
	ErrorCodeNotReady           ErrorCode = 9704

	// ErrorCode_RESERVE 98xx.

	// ErrorCode_OTHERS 99xx.
	ErrorCodeResourceExhausted ErrorCode = 9901
	ErrorCodeResourceCanNotOp  ErrorCode = 9902

	ErrorCodeUnauthenticated  ErrorCode = 9910
	ErrorCodePermissionDenied ErrorCode = 9911
)

var (
	// UNKNOWN.
	ErrUnknown = New("unknown").WithGRPCCode(ErrorCodeUnknown)

	// RESOURCE_NOT_FOUND.
	ErrResourceNotFound       = New("resource not found").WithGRPCCode(ErrorCodeResourceNotFound)
	ErrEventlogNotFound       = New("eventlog not found").WithGRPCCode(ErrorCodeEventlogNotFound)
	ErrSegmentNotFound        = New("segment not found").WithGRPCCode(ErrorCodeSegmentNotFound)
	ErrBlockNotFound          = New("block not found").WithGRPCCode(ErrorCodeBlockNotFound)
	ErrVolumeInstanceNotFound = New("volume instance not found").WithGRPCCode(ErrorCodeVolumeNotFound)

	// SERVICE_NOT_RUNNING.
	ErrServerNotStart            = New("server not start").WithGRPCCode(ErrorCodeServiceNotRunning)
	ErrSegmentServerHasBeenAdded = New("the segment server has been added").WithGRPCCode(
		ErrorCodeSegmentServerHasBeenAdded)
	ErrServiceState   = New("service state error").WithGRPCCode(ErrorCodeServiceStateError)
	ErrWorkerNotStart = New("worker not start").WithGRPCCode(ErrorCodeWorkerNotRunning)

	// SEGMENT_FULL.
	ErrSegmentFull           = New("segment full").WithGRPCCode(ErrorCodeSegmentFull)
	ErrSegmentNotEnoughSpace = New("not enough space").WithGRPCCode(ErrorCodeSegmentNoEnoughSpace)
	ErrOffsetUnderflow       = New("the offset underflow").WithGRPCCode(ErrorCodeOffsetUnderflow)
	ErrOffsetOverflow        = New("the offset overflow").WithGRPCCode(ErrorCodeOffsetOverflow)
	ErrOffsetOnEnd           = New("the offset on end").WithGRPCCode(ErrorCodeOffsetOnEnd)
	ErrBlockNotSupported     = New("block not supported").WithGRPCCode(ErrorCodeBlockNotSupported)
	ErrNotWritable           = New("not writable").WithGRPCCode(ErrorCodeNotWritable)
	ErrNotReadable           = New("not readable").WithGRPCCode(ErrorCodeNotReadable)
	ErrTryAgain              = New("try again").WithGRPCCode(ErrorCodeTryAgain)
	ErrNoEndpoint            = New("no endpoint").WithGRPCCode(ErrorCodeNoEndpoint)
	ErrClosed                = New("closed").WithGRPCCode(ErrorCodeClosed)

	// INTERNAL.
	ErrInternal               = New("internal error").WithGRPCCode(ErrorCodeInternal)
	ErrTriggerWorker          = New("trigger worker error").WithGRPCCode(ErrorCodeTriggerWorker)
	ErrInvalidSegment         = New("invalid segment").WithGRPCCode(ErrorCodeInvalidSegment)
	ErrInvalidHeartBeat       = New("invalid heartbeat").WithGRPCCode(ErrorCodeInvalidHeartbeat)
	ErrVolumeInstanceNoServer = New("no segment server was bound to volume instance").WithGRPCCode(ErrorCodeVolumeNoServer)
	ErrJSONMarshal            = New("json marshal").WithGRPCCode(ErrorCodeJSONMarshal)
	ErrJSONUnMarshal          = New("json unmarshal").WithGRPCCode(ErrorCodeJSONUnmarshal)
	ErrAESEncrypt             = New("aes encrypt").WithGRPCCode(ErrorCodeAESEncrypt)
	ErrAESDecrypt             = New("aes decrypt").WithGRPCCode(ErrorCodeAESDecrypt)
	ErrUnmarshal              = New("unmarshal data failed").WithGRPCCode(ErrorCodeUnmarshal)
	ErrLambdaInvoke           = New("lambda invoke error").WithGRPCCode(ErrorCodeLambdaInvoke)
	ErrLambdaInvokeResponse   = New("lambda invoke response fail").WithGRPCCode(ErrorCodeLambdaInvokeResponse)
	ErrInvalidJSONPath        = New("invalid JSON path").WithGRPCCode(ErrorCodeInvalidJSONPath)
	ErrJSONPathNotExist       = New("JSON path not exist").WithGRPCCode(ErrorCodeJSONPathNotExist)

	// INVALID_REQUEST.
	ErrInvalidRequest          = New("invalid request").WithGRPCCode(ErrorCodeInvalidRequest)
	ErrInvalidArgument         = New("invalid argument").WithGRPCCode(ErrorCodeInvalidArgument)
	ErrCeSQLExpression         = New("ce sql expression invalid").WithGRPCCode(ErrorCodeCESQLExpression)
	ErrCelExpression           = New("cel expression invalid").WithGRPCCode(ErrorCodeCELExpression)
	ErrFilterAttributeIsEmpty  = New("filter dialect attribute is empty").WithGRPCCode(ErrorCodeFilterAttributeIsEmpty)
	ErrFilterMultiple          = New("filter multiple dialects found").WithGRPCCode(ErrorCodeFilterMultiple)
	ErrInvalidHeartBeatRequest = New("invalid heartbeat request").WithGRPCCode(ErrorCodeInvalidHeartbeatRequest)
	ErrVanusJSONParse          = New("invalid json").WithGRPCCode(ErrorCodeJSONParse)
	ErrTransformInputParse     = New("transform input invalid").WithGRPCCode(ErrorCodeTransformInputParse)
	ErrCorruptedEvent          = New("corrupted event").WithGRPCCode(ErrorCodeCorruptedEvent)

	// ErrResourceAlreadyExist.
	ErrResourceAlreadyExist = New("resource already exist").WithGRPCCode(ErrorCodeResourceExist)

	// ErrNotLeader not leader.
	ErrNotLeader          = New("not leader").WithGRPCCode(ErrorCodeNotLeader)
	ErrNotReady           = New("not ready").WithGRPCCode(ErrorCodeNotReady)
	ErrNoControllerLeader = New("no leader controller found").WithGRPCCode(ErrorCodeNoControllerLeader)
	ErrNotRaftLeader      = New("the node is not raft leader").WithGRPCCode(ErrorCodeNotRaftLeader)

	// RESOURCE_EXHAUSTED.
	ErrNoAvailableEventlog = New("no eventlog available").WithGRPCCode(ErrorCodeResourceExhausted)

	// NO_MORE_MESSAGE.

	// RESOURCE_CAN_NOT_OP.
	ErrResourceCanNotOp = New("resource can not operation").WithGRPCCode(ErrorCodeResourceCanNotOp)

	ErrUnauthenticated  = New("unauthenticated").WithGRPCCode(ErrorCodeUnauthenticated)
	ErrPermissionDenied = New("permissionDenied").WithGRPCCode(ErrorCodePermissionDenied)
)
