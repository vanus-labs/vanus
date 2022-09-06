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

package client

import (
	"context"
	"errors"

	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
)

type http struct {
	client ce.Client
}

func NewHTTPClient(url string) EventClient {
	c, _ := ce.NewClientHTTP(ce.WithTarget(url))
	return &http{
		client: c,
	}
}

func (c *http) Send(ctx context.Context, event ce.Event) Result {
	res := c.client.Send(ctx, event)
	if ce.IsACK(res) {
		return Success
	}
	if errors.Is(res, context.DeadlineExceeded) {
		return DeliveryTimeout
	}
	r := Result{Err: res}
	var httpResult *cehttp.Result
	if ce.ResultAs(res, &httpResult) {
		r.StatusCode = httpResult.StatusCode
	} else {
		r.StatusCode = ErrUndefined
	}

	return r
}
