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

package worker

import (
	"context"
	"github.com/linkall-labs/vsproto/pkg/trigger"
)

type server struct {
	tWorker *TWorker
	stopCallback func()
}

func NewTriggerServer(stop func()) trigger.TriggerWorkerServer {
	return &server{
		stopCallback: stop,
	}
}

func (s *server) Start(context.Context, *trigger.StartTriggerWorkerRequest) (*trigger.StartTriggerWorkerResponse, error) {
	return nil, nil
}

func (s *server) Stop(context.Context, *trigger.StopTriggerWorkerRequest) (*trigger.StopTriggerWorkerResponse, error) {
	s.stopCallback()
	return nil, nil
}

func (s *server) AddSubscription(context.Context, *trigger.AddSubscriptionRequest) (*trigger.AddSubscriptionResponse, error) {
	return nil, nil
}

func (s *server) RemoveSubscription(context.Context, *trigger.RemoveSubscriptionRequest) (*trigger.RemoveSubscriptionResponse, error) {
	return nil, nil
}

func (s *server) PauseSubscription(context.Context, *trigger.PauseSubscriptionRequest) (*trigger.PauseSubscriptionResponse, error) {
	return nil, nil
}

func (s *server) ResumeSubscription(context.Context, *trigger.ResumeSubscriptionRequest) (*trigger.ResumeSubscriptionResponse, error) {
	return nil, nil
}
