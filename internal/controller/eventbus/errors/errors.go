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
	ErrEventLogNotFound       = rpcerr.New("eventlog not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
	ErrUnmarshall             = rpcerr.New("unmarshall data failed").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrNoAvailableEventLog    = rpcerr.New("no eventlog available").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_EXHAUSTED)
	ErrVolumeInstanceNotFound = rpcerr.New("volume instance not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
	ErrVolumeInstanceNoServer = rpcerr.New("no segment server was bound to volume instance").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrInvalidSegment         = rpcerr.New("invalid segment").WithGRPCCode(rpcerr.ErrorCode_INTERNAL)
	ErrSegmentNotFound        = rpcerr.New("segment not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
	ErrBlockNotFound          = rpcerr.New("block not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)

	ErrSegmentServerHasBeenAdded = rpcerr.New("the segment server has been added").WithGRPCCode(rpcerr.ErrorCode_SERVICE_NOT_RUNNING)
)
