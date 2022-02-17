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
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/trigger"
)

type server struct {
	tWorker      *TWorker
	stopCallback func()
}

func NewTriggerServer(stop func()) trigger.TriggerWorkerServer {
	return &server{
		stopCallback: stop,
		tWorker:      &TWorker{},
	}
}

func (s *server) Start(ctx context.Context, request *trigger.StartTriggerWorkerRequest) (*trigger.StartTriggerWorkerResponse, error) {
	log.Info("worker server start ", map[string]interface{}{"request": request})
	err := s.tWorker.Start()
	if err != nil {
		return nil, err
	}
	return &trigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(context.Context, *trigger.StopTriggerWorkerRequest) (*trigger.StopTriggerWorkerResponse, error) {
	s.stopCallback()
	return &trigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context, request *trigger.AddSubscriptionRequest) (*trigger.AddSubscriptionResponse, error) {
	log.Debug("subscription add ", map[string]interface{}{"request": request})
	return &trigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context, request *trigger.RemoveSubscriptionRequest) (*trigger.RemoveSubscriptionResponse, error) {
	log.Debug("subscription remove ", map[string]interface{}{"request": request})
	return &trigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context, request *trigger.PauseSubscriptionRequest) (*trigger.PauseSubscriptionResponse, error) {
	log.Debug("subscription pause ", map[string]interface{}{"request": request})
	return &trigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context, request *trigger.ResumeSubscriptionRequest) (*trigger.ResumeSubscriptionResponse, error) {
	log.Debug("subscription resume ", map[string]interface{}{"request": request})
	return &trigger.ResumeSubscriptionResponse{}, nil
}
