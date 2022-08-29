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

import rpcerr "github.com/linkall-labs/vanus/proto/pkg/errors"

var (
	ErrInvalidRequest       = rpcerr.New("invalid request").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrResourceNotFound     = rpcerr.New("resource not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
	ErrResourceAlreadyExist = rpcerr.New("resource already exist").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_EXIST)
	ErrWorkerNotStart       = rpcerr.New("worker not start").WithGRPCCode(rpcerr.ErrorCode_SERVICE_NOT_RUNNING)
	ErrInternal             = rpcerr.New("internal").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)

	ErrVanusJSONParse       = rpcerr.New("invalid json").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrTransformInputParse  = rpcerr.New("transform input invalid").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrLambdaInvokeResponse = rpcerr.New("lambda invoke response fail").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrLambdaInvoke         = rpcerr.New("lambda invoke error").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)

	ErrNoControllerLeader = rpcerr.New("no leader controller found").WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
)
