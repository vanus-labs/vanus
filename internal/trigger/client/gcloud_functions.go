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
	"bytes"
	"context"
	"errors"
	nethttp "net/http"
	"sync"

	ce "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

type gcloudFunctions struct {
	client         *nethttp.Client
	url            string
	credentialJSON string
	lock           sync.Mutex
}

func NewGCloudFunctionClient(url, credentialJSON string) EventClient {
	return &gcloudFunctions{
		url:            url,
		credentialJSON: credentialJSON,
	}
}

func (c *gcloudFunctions) init(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.client != nil {
		return nil
	}
	client, err := idtoken.NewClient(ctx, c.url, option.WithCredentialsJSON([]byte(c.credentialJSON)))
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *gcloudFunctions) Send(ctx context.Context, events ...*ce.Event) Result {
	event := events[0]
	if c.client == nil {
		err := c.init(ctx)
		if err != nil {
			return newUndefinedErr(err)
		}
	}
	payload, err := event.MarshalJSON()
	if err != nil {
		return newInternalErr(err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return newUndefinedErr(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return DeliveryTimeout
		}
		return newUndefinedErr(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= errStatusCode {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return convertHTTPResponse(resp.StatusCode, "gcloud functions invoke", buf.Bytes())
	}
	return Success
}
