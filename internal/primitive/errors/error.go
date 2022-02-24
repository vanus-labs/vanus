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

import "github.com/pkg/errors"

type ErrorCode int

const (
	NotBeenClassified = 0
)

type Notice string

func ConvertGRPCError(code ErrorCode, notice Notice, errs ...error) error {
	return nil
}

func Chain(errs ...error) error {
	// TODO optimize when idx=0 is nil
	if len(errs) == 0 {
		return nil
	}
	newErr := errs[0]
	for idx := 1; idx < len(errs); idx++ {
		newErr = errors.Wrap(newErr, errs[idx].Error())
	}
	return newErr
}
