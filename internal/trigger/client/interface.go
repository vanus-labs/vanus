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

//go:generate mockgen -source=interface.go  -destination=mock_interface.go -package=client
package client

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"

	ce "github.com/cloudevents/sdk-go/v2"
)

type Sender interface {
	Send(ctx context.Context, event ce.Event) Result
}

type EventClient interface {
	Sender
}

type Result struct {
	StatusCode int
	Err        error
}

func newResultByHTTPCode(httpCode int) Result {
	return Result{
		StatusCode: httpCode,
		Err:        fmt.Errorf("%d %s", httpCode, nethttp.StatusText(httpCode)),
	}
}

func newInternalErr(err error) Result {
	return Result{
		StatusCode: nethttp.StatusInternalServerError,
		Err:        err,
	}
}

func newUndefinedErr(err error) Result {
	return Result{
		StatusCode: ErrUndefined,
		Err:        err,
	}
}

func convertHTTPResponse(statusCode int, desc string, body []byte) Result {
	switch statusCode {
	case nethttp.StatusForbidden:
		return Forbidden
	case nethttp.StatusRequestEntityTooLarge:
		return RequestEntityTooLarge
	default:
		return Result{
			StatusCode: statusCode,
			Err:        fmt.Errorf("%s response statusCode: %d, body: %s", desc, statusCode, string(body)),
		}
	}
}

var (
	Success               = Result{}
	DeliveryTimeout       = Result{ErrDeliveryTimeout, errors.New("DeliveryTimeout")}
	Forbidden             = newResultByHTTPCode(nethttp.StatusForbidden)
	RequestEntityTooLarge = newResultByHTTPCode(nethttp.StatusRequestEntityTooLarge)
)

const (
	errStatusCode = 400

	ErrDeliveryTimeout = 601
	ErrUndefined       = 700
)
