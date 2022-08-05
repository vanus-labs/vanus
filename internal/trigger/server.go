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

	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/observability/log"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ pbtrigger.TriggerWorkerServer = &server{}
)

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
	request *pbtrigger.StartTriggerWorkerRequest) (*pbtrigger.StartTriggerWorkerResponse, error) {
	log.Info(ctx, "worker server start ", map[string]interface{}{"request": request})
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
	request *pbtrigger.StopTriggerWorkerRequest) (*pbtrigger.StopTriggerWorkerResponse, error) {
	log.Info(ctx, "worker server stop ", map[string]interface{}{"request": request})
	s.stop(context.Background(), false)
	os.Exit(1)
	return &pbtrigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context,
	request *pbtrigger.AddSubscriptionRequest) (*pbtrigger.AddSubscriptionResponse, error) {
	log.Info(ctx, "subscription add ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	subscription := convert.FromPbAddSubscription(request)
	err := s.worker.AddSubscription(ctx, subscription)
	if err != nil {
		log.Error(ctx, "add subscription error ", map[string]interface{}{
			"subscription": subscription,
			log.KeyError:   err,
		})
		return nil, err
	}
	return &pbtrigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context,
	request *pbtrigger.RemoveSubscriptionRequest) (*pbtrigger.RemoveSubscriptionResponse, error) {
	log.Info(ctx, "subscription remove ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.RemoveSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx, "remove subscription error", map[string]interface{}{
			log.KeySubscriptionID: request.SubscriptionId,
			log.KeyError:          err,
		})
		return nil, err
	}
	return &pbtrigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context,
	request *pbtrigger.PauseSubscriptionRequest) (*pbtrigger.PauseSubscriptionResponse, error) {
	log.Info(ctx, "subscription pause ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.PauseSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx, "pause subscription error", map[string]interface{}{
			log.KeySubscriptionID: request.SubscriptionId,
			log.KeyError:          err,
		})
		return nil, err
	}
	return &pbtrigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context,
	request *pbtrigger.ResumeSubscriptionRequest) (*pbtrigger.ResumeSubscriptionResponse, error) {
	log.Info(ctx, "subscription resume ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.StartSubscription(ctx, vanus.NewIDFromUint64(request.SubscriptionId))
	if err != nil {
		log.Error(ctx, "resume subscription error", map[string]interface{}{
			log.KeySubscriptionID: request.SubscriptionId,
			log.KeyError:          err,
		})
		return nil, err
	}
	return &pbtrigger.ResumeSubscriptionResponse{}, nil
}

func (s *server) ResetOffsetToTimestamp(ctx context.Context,
	request *pbtrigger.ResetOffsetToTimestampRequest) (*emptypb.Empty, error) {
	log.Info(ctx, "subscription reset offset ", map[string]interface{}{"request": request})
	id := vanus.NewIDFromUint64(request.SubscriptionId)
	err := s.worker.ResetOffsetToTimestamp(ctx, id, int64(request.Timestamp))
	if err != nil {
		log.Error(ctx, "reset offset error", map[string]interface{}{
			log.KeySubscriptionID: id,
			log.KeyError:          err,
		})
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *server) Initialize(ctx context.Context) error {
	err := s.worker.Register(ctx)
	if err != nil {
		log.Error(ctx, "register trigger worker error", map[string]interface{}{
			"tcAddr":     s.config.ControllerAddr,
			log.KeyError: err,
		})
		return err
	}
	log.Info(ctx, "trigger worker register success", map[string]interface{}{
		"triggerCtrlAddr":   s.config.ControllerAddr,
		"triggerWorkerAddr": s.config.TriggerAddr,
	})
	s.state = primitive.ServerStateStarted
	s.startTime = time.Now()
	return nil
}

func (s *server) Close(ctx context.Context) error {
	log.Info(ctx, "trigger worker server stop...", nil)
	s.stop(ctx, true)
	log.Info(ctx, "trigger worker server stopped", nil)
	return nil
}

func (s *server) stop(ctx context.Context, sendUnregister bool) {
	if s.state != primitive.ServerStateRunning {
		return
	}
	err := s.worker.Stop(ctx)
	if err != nil {
		log.Error(ctx, "trigger worker stop error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	if sendUnregister {
		err = s.worker.Unregister(ctx)
		if err != nil {
			log.Error(ctx, "unregister trigger worker error", map[string]interface{}{
				"addr":       s.config.ControllerAddr,
				log.KeyError: err,
			})
		} else {
			log.Info(ctx, "unregister trigger worker success", nil)
		}
	}
	s.state = primitive.ServerStateStopped
}
