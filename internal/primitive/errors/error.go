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

var (
	NotBeenClassified = errorCode{code: 0, message: "error has not been classified"}

	ErrServiceUnavailable = errorCode{code: 1001, message: "service unavailable"}
	ErrPermissionDenied   = errorCode{code: 1002, message: "permission denied"}
	ErrNoServerAvailable  = errorCode{code: 1003, message: "no server available"}

	ErrCreateEventbus     = errorCode{code: 2001, message: "failed to create eventbus"}
	ErrCreateSubscription = errorCode{code: 2002, message: "failed to create subscription"}
	ErrAllocateSegment    = errorCode{code: 2003, message: "failed to allocate segment"}

	ErrSegmentNoEnoughCapacity = errorCode{code: 3001, message: "no enough capacity"}
	ErrSegmentNotFound         = errorCode{code: 3002, message: "segment not found"}
	ErrVolumeIsFull            = errorCode{code: 3003, message: "volume is full"}

	// TriggerWorker
	// Connector
	// Gateway
)

type errorCode struct {
	code           int
	message        string
	explanation    string
	underlayErrors []error
}

// Equals is error gaven equals to the errorCode. the errorCode.code is only be compared
func (e *errorCode) Equals(err error) bool {
	v, ok := err.(errorCode)
	if !ok {
		return false
	}
	return e.code == v.code
}

// Explain add additional message to explain why this error occurred.
// the explanation was used to improve understandability of the error in order to make
// people know what they should do
func (e *errorCode) Explain(str string) {
	e.explanation = str
}

// Wrap the other error as the underlay errors of this error. sometimes we return an error because
// of another error(named underlay error). So, we should add the underlay error to this error's context.
// By this, the people can understand why this error they received
func (e *errorCode) Wrap(err error) {
	if err == nil || err.Error() == "" {
		return
	}
	if e.underlayErrors == nil {
		e.underlayErrors = make([]error, 0)
	}
	e.underlayErrors = append(e.underlayErrors, err)
}

// Error return readable error message by string
func (e errorCode) Error() string {
	str := fmt.Sprintf("{\"code\":%d, \"message\": \"%s\"", e.code, e.message)
	if e.explanation != "" {
		str = fmt.Sprintf("%s, \"explanation\": \"%s\"", str, e.explanation)
	}
	for idx := range e.underlayErrors {
		v := e.underlayErrors[idx]
		switch v.(type) {
		case errorCode:
			str = fmt.Sprintf("%s, \"err%d\": %s", str, idx, e.underlayErrors[idx])
		default:
			str = fmt.Sprintf("%s, \"err%d\": \"%s\"", str, idx, e.underlayErrors[idx])
		}
	}

	return str + "}"
}

type Notice string

// ConvertGRPCError convert an internal error to an exported error in gRPC
//
// Deprecated: please use errorCode instead of this
func ConvertGRPCError(code errorCode, notice Notice, errs ...error) error {
	return fmt.Errorf("code: %d, notice: %s, error:%s", code.code, notice, errs)
}

// Chain method can group many errors as a single error. this is a helpful method there are many errors
// will be occurred in one method.
//
// Example: if we have a loop in one method, and each iterator create a goroutine, each goroutine may
// an error occurred, but we have to wait all goroutine done, in this situation, wo can use this method
// to collect all errors occurred in different goroutine.
//
// Notice: this method is not CONCURRENCY SAFETY. you can see somewhere call this function to know more
// details about how to use this method.
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
