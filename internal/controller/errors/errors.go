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
	ErrInternal             = rpcerr.New("internal error").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrResourceCanNotOp     = rpcerr.New("resource can not operation").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_CAN_NOT_OP)

	ErrServerNotStart = rpcerr.New("server not start").WithGRPCCode(rpcerr.ErrorCode_SERVICE_NOT_RUNNING)
	ErrJSONMarshal    = rpcerr.New("json marshal").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrJSONUnMarshal  = rpcerr.New("json unmarshal").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrAESEncrypt     = rpcerr.New("aes encrypt").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrAESDecrypt     = rpcerr.New("aes decrypt").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)

	ErrTriggerWorker = rpcerr.New("trigger worker error").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)

	ErrCeSQLExpression        = rpcerr.New("ce sql expression invalid").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrCelExpression          = rpcerr.New("cel expression invalid").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrFilterAttributeIsEmpty = rpcerr.New("filter dialect attribute is empty").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)
	ErrFilterMultiple         = rpcerr.New("filter multiple dialects found").WithGRPCCode(rpcerr.ErrorCode_INVALID_REQUEST)

	ErrNotLeader = rpcerr.New("not leader").WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
)
