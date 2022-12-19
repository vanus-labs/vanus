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
	ErrUnknown = New("unknown").WithGRPCCode(errpb.ErrorCode_UNKNOWN)

	// INVALID_REQUEST
	ErrInvalidRequest      = New("invalid request").WithGRPCCode(errpb.ErrorCode_INVALID_REQUEST)
	ErrTransformInputParse = New("transform input invalid").WithGRPCCode(errpb.ErrorCode_TRANSFORM_INPUT_PARSE)
	ErrNoEndpoint          = New("no endpoint").WithGRPCCode(errpb.ErrorCode_NO_ENDPOINT)

	// INTERNAL
	ErrInternal = New("internal error").WithGRPCCode(errpb.ErrorCode_INTERNAL)

	// FULL
	ErrFull            = New("full").WithGRPCCode(errpb.ErrorCode_FULL)
	ErrNotWritable     = New("not writable").WithGRPCCode(errpb.ErrorCode_NOT_WRITABLE)
	ErrNotReadable     = New("not readable").WithGRPCCode(errpb.ErrorCode_NOT_READABLE)
	ErrOffsetOnEnd     = New("the offset on end").WithGRPCCode(errpb.ErrorCode_OFFSET_ON_END)
	ErrOffsetOverflow  = New("the offset overflow").WithGRPCCode(errpb.ErrorCode_OFFSET_OVERFLOW)
	ErrOffsetUnderflow = New("the offset underflow").WithGRPCCode(errpb.ErrorCode_OFFSET_UNDERFLOW)

	// RESOURCE_NOT_FOUND
	ErrResourceNotFound = New("resource not found").WithGRPCCode(errpb.ErrorCode_RESOURCE_NOT_FOUND)

	// RESOURCE_EXIST
	ErrResourceAlreadyExist = New("resource already exist").WithGRPCCode(errpb.ErrorCode_RESOURCE_EXIST)

	// SERVICE_NOT_RUNNING
	ErrServerNotRunning = New("server not running").WithGRPCCode(errpb.ErrorCode_SERVICE_NOT_RUNNING)
	ErrClosed           = New("closed").WithGRPCCode(errpb.ErrorCode_CLOSED)

	// NO_LEADER
	ErrNotLeader          = New("not leader").WithGRPCCode(errpb.ErrorCode_NOT_LEADER)
	ErrNoControllerLeader = New("no leader controller found").WithGRPCCode(errpb.ErrorCode_NO_CONTROLLER_LEADER)
)
