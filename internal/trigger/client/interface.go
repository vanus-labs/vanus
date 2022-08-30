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

	ce "github.com/cloudevents/sdk-go/v2"
)

type Sender interface {
	Send(ctx context.Context, event ce.Event) Result
}

type Result struct {
	StatusCode int
	Err        error
}

var Success Result

const (
	ErrInternalCode = 600
	ErrHttp         = 650
	ErrLambdaInvoke = 700
)

type EventClient interface {
	Sender
}
