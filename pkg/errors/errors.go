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

import (
	errpb "github.com/linkall-labs/vanus/proto/pkg/errors"
)

var (
	// UNKNOWN
	ErrUnknown = New("unknown").WithCode(errpb.ErrorCode_UNKNOWN)

	// INVALID_REQUEST
	ErrInvalidRequest      = New("invalid request").WithCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrTransformInputParse = New("transform input invalid").WithCode(errpb.ErrorCode_PARSE_TRANSFORM_INPUT)
	ErrNoEndpoint          = New("no endpoint").WithCode(errpb.ErrorCode_NO_ENDPOINT)

	// INTERNAL
	ErrInternal       = New("internal error").WithCode(errpb.ErrorCode_INTERNAL)
	ErrCorruptedEvent = New("corrupted event").WithCode(errpb.ErrorCode_CORRUPTED_EVENT)
	ErrAppendToBlock  = New("append to block failed").WithCode(errpb.ErrorCode_APPEND_TO_BLOCK_FAILED)
	ErrReadFromBlock  = New("read from block failed").WithCode(errpb.ErrorCode_READ_FROM_BLOCK_FAILED)

	// FULL
	ErrFull            = New("full").WithCode(errpb.ErrorCode_FULL)
	ErrNotWritable     = New("not writable").WithCode(errpb.ErrorCode_NOT_WRITABLE)
	ErrNotReadable     = New("not readable").WithCode(errpb.ErrorCode_NOT_READABLE)
	ErrOffsetOnEnd     = New("the offset on end").WithCode(errpb.ErrorCode_OFFSET_ON_END)
	ErrOffsetOverflow  = New("the offset overflow").WithCode(errpb.ErrorCode_OFFSET_OVERFLOW)
	ErrOffsetUnderflow = New("the offset underflow").WithCode(errpb.ErrorCode_OFFSET_UNDERFLOW)
	ErrTryAgain        = New("try again").WithCode(errpb.ErrorCode_TRY_AGAIN)

	// RESOURCE
	ErrResourceAlreadyExist = New("resource already exist").WithCode(errpb.ErrorCode_RESOURCE_ALREADY_EXIST)
	ErrResourceNotFound     = New("resource not found").WithCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)
	ErrResourceNotReady     = New("resource not ready").WithCode(errpb.ErrorCode_RESOURCE_NOT_READY)
	ErrResourceCanNotOp     = New("resource can not operation").WithCode(errpb.ErrorCode_RESOURCE_CAN_NOT_OP)

	// SERVICE_NOT_RUNNING
	ErrServerNotRunning = New("server not running").WithCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)
	ErrClosed           = New("closed").WithCode(errpb.ErrorCode_CLOSED)

	// NO_LEADER
	ErrNoLeader           = New("no leader").WithCode(errpb.ErrorCode_NO_LEADER)
	ErrNotLeader          = New("not leader").WithCode(errpb.ErrorCode_NOT_LEADER)
	ErrNoControllerLeader = New("no leader controller found").WithCode(errpb.ErrorCode_NO_CONTROLLER_LEADER)
)
