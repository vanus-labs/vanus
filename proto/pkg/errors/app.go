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
	"encoding/json"
	"fmt"
)

func New(desc string) *ErrorType {
	return &ErrorType{
		Description: desc,
	}
}

func Convert(str string) (*ErrorType, bool) {
	et := &ErrorType{}
	if err := json.Unmarshal([]byte(str), et); err != nil {
		return nil, false
	}
	return et, true
}

type ErrorType struct {
	Description    string    `json:"description"`
	Message        string    `json:"message"`
	Code           ErrorCode `json:"code"`
	underlayErrors []error
}

func (e *ErrorType) WithGRPCCode(c ErrorCode) *ErrorType {
	_e := e.copy()
	_e.Code = c
	return _e
}

// WithMessage add additional message to explain what try to do cause this error.
// the explanation was used to improve understandability of the error in order to make
// people know what they should do
func (e *ErrorType) WithMessage(str string) *ErrorType {
	_e := e.copy()
	_e.Message = str
	return _e
}

// Wrap the other error as the underlay errors of this error. sometimes we return an error because
// of another error(named underlay error). So, we should add the underlay error to this error's context.
// By this, the people can understand why this error they received
func (e *ErrorType) Wrap(err error) *ErrorType {
	if err == nil || err.Error() == "" {
		return nil
	}
	v, ok := err.(ErrorType)
	if ok {
		if v.Code == e.Code && v.Message == "" {
			return e
		}
	}
	_e := e.copy()
	_e.underlayErrors = append(_e.underlayErrors, err)
	return _e
}

// GRPCError convert ErrorType to a gRPC error, if ErrorType.Code hasn't been set,
// the default gRPC error is UNKNOWN.
func (e *ErrorType) GRPCError() *Error {
	er := &Error{
		Message: e.Message,
		Code:    e.Code,
	}
	if e.Code == ErrorCode_UNKNOWN {
		e.Code = ErrorCode_INTERNAL
	}
	return er
}

func (e *ErrorType) JSON() string {
	data, _ := json.Marshal(e)
	return string(data)
}

func (e *ErrorType) copy() *ErrorType {
	errs := make([]error, len(e.underlayErrors))
	copy(errs, e.underlayErrors)
	return &ErrorType{
		Description:    e.Description,
		Message:        e.Message,
		Code:           e.Code,
		underlayErrors: errs,
	}
}

// Error return readable error message by JSON format
func (e ErrorType) Error() string {
	str := fmt.Sprintf("{\"description\": \"%s\",\"code\":%d", e.Description, e.Code)
	if e.Message != "" {
		str = fmt.Sprintf("%s, \"message\": \"%s\"", str, e.Message)
	}

	for idx := range e.underlayErrors {
		v := e.underlayErrors[idx]
		switch v.(type) {
		case ErrorType:
			str = fmt.Sprintf("%s, \"description %d\": %s", str, idx, e.underlayErrors[idx])
		default:
			str = fmt.Sprintf("%s, \"description %d\": \"%s\"", str, idx, e.underlayErrors[idx])
		}
	}

	return str + "}"
}
