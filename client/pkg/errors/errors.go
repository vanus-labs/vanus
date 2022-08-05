// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors

import "errors"

var (
	ErrUnknown            = errors.New("unknown")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrTryAgain           = errors.New("try again")
	ErrOverflow           = errors.New("overflow")
	ErrUnderflow          = errors.New("underflow")
	ErrNotFound           = errors.New("not found")
	ErrNotSupported       = errors.New("not supported")
	ErrNotWritable        = errors.New("not writable")
	ErrNotReadable        = errors.New("not readable")
	ErrNotEnoughSpace     = errors.New("not enough space")
	ErrNoSpace            = errors.New("no space")
	ErrOnEnd              = errors.New("on end")
	ErrClosed             = errors.New("closed")
	ErrNoControllerLeader = errors.New("no controller leader")
	ErrNoBlock            = errors.New("no block")
	ErrNoLeader           = errors.New("no leader")
	ErrNoEndpoint         = errors.New("no endpoint")
	ErrCorruptedEvent     = errors.New("corrupted event")
	ErrTimeout            = errors.New("timeout")
)
