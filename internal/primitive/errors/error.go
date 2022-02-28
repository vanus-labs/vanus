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
	"fmt"
	"github.com/pkg/errors"
)

type ErrorCode int

const (
	NotBeenClassified = 0
)

type Notice string

func ConvertGRPCError(code ErrorCode, notice Notice, errs ...error) error {
	return fmt.Errorf("code: %d, notice: %s, error:%s", code, notice, errs)
}

func Chain(errs ...error) error {
	// TODO optimize when idx=0 is nil
	if len(errs) == 0 {
		return nil
	}
	var err error
	var idx = 0
	for ; idx < len(errs); idx++ {
		if errs[idx] != nil {
			err = errs[idx]
			break
		}
	}
	for _idx := idx + 1; _idx < len(errs); _idx++ {
		if errs[_idx] == nil {
			continue
		}
		err = errors.Wrap(err, errs[_idx].Error())
	}
	return err
}
