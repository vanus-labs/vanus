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
	"encoding/json"
	"fmt"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc/status"
)

type server struct {
	worker       *Worker
	stopCallback func()
}

func NewTriggerServer(stop func()) trigger.TriggerWorkerServer {
	return &server{
		stopCallback: stop,
		worker:       NewWorker(),
	}
}

func (s *server) Start(ctx context.Context, request *trigger.StartTriggerWorkerRequest) (*trigger.StartTriggerWorkerResponse, error) {
	log.Info("worker server start ", map[string]interface{}{"request": request})
	err := s.worker.Start()
	if err != nil {
		return nil, err
	}
	return &trigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context, request *trigger.StopTriggerWorkerRequest) (*trigger.StopTriggerWorkerResponse, error) {
	log.Info("worker server stop ", map[string]interface{}{"request": request})
	s.stopCallback()
	s.worker.Stop()
	return &trigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context, request *trigger.AddSubscriptionRequest) (*trigger.AddSubscriptionResponse, error) {
	log.Info("subscription add ", map[string]interface{}{"request": request})
	sub, err := func(request *trigger.AddSubscriptionRequest) (*primitive.Subscription, error) {
		b, err := json.Marshal(request.Subscription)
		if err != nil {
			return nil, fmt.Errorf("json marshal error %v", err)
		}
		sub := &primitive.Subscription{}
		err = json.Unmarshal(b, sub)
		if err != nil {
			return nil, fmt.Errorf("json unmarshal error %s %v", string(b), err)
		}
		return sub, nil
	}(request)
	if err != nil {
		log.Info("trigger subscription request to subscription error", map[string]interface{}{"error": err})
		return nil, err
	}
	err = s.worker.AddSubscription(sub)
	if err != nil {
		log.Warning("worker add subscription error ", map[string]interface{}{"subscription": sub, "error": err})
		return nil, status.Error(errors.WorkerNotStart, err.Error())
	}
	return &trigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context, request *trigger.RemoveSubscriptionRequest) (*trigger.RemoveSubscriptionResponse, error) {
	log.Info("subscription remove ", map[string]interface{}{"request": request})
	s.worker.RemoveSubscription(request.Id)
	return &trigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context, request *trigger.PauseSubscriptionRequest) (*trigger.PauseSubscriptionResponse, error) {
	log.Info("subscription pause ", map[string]interface{}{"request": request})
	s.worker.PauseSubscription(request.Id)
	return &trigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context, request *trigger.ResumeSubscriptionRequest) (*trigger.ResumeSubscriptionResponse, error) {
	log.Debug("subscription resume ", map[string]interface{}{"request": request})
	return &trigger.ResumeSubscriptionResponse{}, nil
}
