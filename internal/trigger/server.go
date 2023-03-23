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

package trigger

import (
	"context"
	"os"
	"time"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/errors"
	pbtrigger "github.com/vanus-labs/vanus/proto/pkg/trigger"

	"github.com/vanus-labs/vanus/internal/convert"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

var _ pbtrigger.TriggerWorkerServer = &server{}

type server struct {
	worker    Worker
	config    Config
	state     primitive.ServerState
	startTime time.Time
}

func NewTriggerServer(config Config) pbtrigger.TriggerWorkerServer {
	s := &server{
		config: config,
		worker: NewWorker(config),
		state:  primitive.ServerStateCreated,
	}
	return s
}

func (s *server) Start(ctx context.Context,
	_ *pbtrigger.StartTriggerWorkerRequest,
) (*pbtrigger.StartTriggerWorkerResponse, error) {
	log.Info(ctx).Msg("worker server start ")
	if s.state == primitive.ServerStateRunning {
		return &pbtrigger.StartTriggerWorkerResponse{}, nil
	}
	err := s.worker.Start(ctx)
	if err != nil {
		return nil, err
	}
	s.state = primitive.ServerStateRunning
	return &pbtrigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context,
	_ *pbtrigger.StopTriggerWorkerRequest,
) (*pbtrigger.StopTriggerWorkerResponse, error) {
	log.Info(ctx).Msg("worker server stop ")
	s.stop(context.Background(), false)
	os.Exit(1)
	return &pbtrigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context,
	request *pbtrigger.AddSubscriptionRequest,
) (*pbtrigger.AddSubscriptionResponse, error) {
	log.Info(ctx).Msg("subscription add ")
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	subscription := convert.FromPbAddSubscription(request)
	log.Info(ctx).Msg("subscription add info ")
	err := s.worker.AddSubscription(ctx, subscription)
	if err != nil {
		log.Error(ctx).Err(err).
			Interface("subscription", subscription).
			Msg("add subscription error ")
		return nil, err
	}
	return &pbtrigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context,
	request *pbtrigger.RemoveSubscriptionRequest,
) (*pbtrigger.RemoveSubscriptionResponse, error) {
	log.Info(ctx).Msg("subscription remove ")
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.RemoveSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx).Err(err).
			Uint64(log.KeySubscriptionID, request.SubscriptionId).
			Msg("remove subscription error")
		return nil, err
	}
	return &pbtrigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context,
	request *pbtrigger.PauseSubscriptionRequest,
) (*pbtrigger.PauseSubscriptionResponse, error) {
	log.Info(ctx).Msg("subscription pause ")
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.PauseSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx).Err(err).
			Uint64(log.KeySubscriptionID, request.SubscriptionId).
			Msg("pause subscription error")
		return nil, err
	}
	return &pbtrigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context,
	request *pbtrigger.ResumeSubscriptionRequest,
) (*pbtrigger.ResumeSubscriptionResponse, error) {
	log.Info(ctx).Msg("subscription resume ")
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.StartSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx).Err(err).
			Uint64(log.KeySubscriptionID, request.SubscriptionId).
			Msg("resume subscription error")
		return nil, err
	}
	return &pbtrigger.ResumeSubscriptionResponse{}, nil
}

func (s *server) Initialize(ctx context.Context) error {
	err := s.worker.Init(ctx)
	if err != nil {
		log.Error(ctx).Err(err).Msg("worker init error")
		return err
	}
	err = s.worker.Register(ctx)
	if err != nil {
		log.Error(ctx).Err(err).
			Strs("tcAddr", s.config.ControllerAddr).
			Msg("register trigger worker error")
		return err
	}
	log.Info(ctx).
		Strs("tcAddr", s.config.ControllerAddr).
		Str("triggerWorkerAddr", s.config.TriggerAddr).
		Msg("trigger worker register success")
	s.state = primitive.ServerStateStarted
	s.startTime = time.Now()
	return nil
}

func (s *server) Close(ctx context.Context) error {
	log.Info(ctx).Msg("trigger worker server stop...")
	s.stop(ctx, true)
	log.Info(ctx).Msg("trigger worker server stopped")
	return nil
}

func (s *server) stop(ctx context.Context, sendUnregister bool) {
	if s.state != primitive.ServerStateRunning {
		return
	}
	err := s.worker.Stop(ctx)
	if err != nil {
		log.Error(ctx).Err(err).Msg("trigger worker stop error")
	}
	if sendUnregister {
		err = s.worker.Unregister(ctx)
		if err != nil {
			log.Error(ctx).Err(err).
				Strs("address", s.config.ControllerAddr).
				Msg("unregister trigger worker error")
		} else {
			log.Info(ctx).Msg("unregister trigger worker success")
		}
	}
	s.state = primitive.ServerStateStopped
}
