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

import rpcerr "github.com/linkall-labs/vsproto/pkg/errors"

var (
	ErrInternal              = rpcerr.New("internal error").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrInvalidRequest        = rpcerr.New("invalid request").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrServiceState          = rpcerr.New("service state error").WithGRPCCode(rpcerr.ErrorCode_SERVICE_NOT_RUNNING)
	ErrSegmentNotEnoughSpace = rpcerr.New("not enough space").WithGRPCCode(rpcerr.ErrorCode_SEGMENT_FULL)
	ErrSegmentFull           = rpcerr.New("full").WithGRPCCode(rpcerr.ErrorCode_SEGMENT_FULL)
	ErrResourceNotFound      = rpcerr.New("resource not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
	ErrResourceAlreadyExist  = rpcerr.New("resource already exist").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_EXIST)
	ErrNoControllerLeader    = rpcerr.New("no leader controller found").WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
)
