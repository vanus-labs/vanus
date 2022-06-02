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
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/trigger/worker"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"
)

type server struct {
	worker    *worker.Worker
	config    Config
	client    *ctrlClient
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	state     primitive.ServerState
	startTime time.Time
}

func NewTriggerServer(config Config) pbtrigger.TriggerWorkerServer {
	s := &server{
		config: config,
		worker: worker.NewWorker(worker.Config{
			Controllers: config.ControllerAddr,
		}),
		client: NewClient(config.ControllerAddr),
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
	err := s.worker.Start()
	if err != nil {
		return nil, err
	}
	s.startHeartbeat()
	s.state = primitive.ServerStateRunning
	return &pbtrigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context,
	request *pbtrigger.StopTriggerWorkerRequest) (*pbtrigger.StopTriggerWorkerResponse, error) {
	log.Info(ctx, "worker server stop ", map[string]interface{}{"request": request})
	s.stop(false)
	os.Exit(1)
	return &pbtrigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context,
	request *pbtrigger.AddSubscriptionRequest) (*pbtrigger.AddSubscriptionResponse, error) {
	log.Info(ctx, "subscription add ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	sub := convert.FromPbAddSubscription(request)
	err := s.worker.AddSubscription(sub)
	if err != nil {
		if err == errors.ErrResourceAlreadyExist {
			log.Info(ctx, "add subscription bus sub exist", map[string]interface{}{
				"id": sub.ID,
			})
		} else {
			log.Warning(ctx, "worker add subscription error ", map[string]interface{}{"subscription": sub, "error": err})
			return nil, err
		}
	}
	return &pbtrigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context,
	request *pbtrigger.RemoveSubscriptionRequest) (*pbtrigger.RemoveSubscriptionResponse, error) {
	log.Info(ctx, "subscription remove ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.RemoveSubscription(vanus.ID(request.SubscriptionId))
	if err != nil {
		log.Info(ctx, "remove subscription error", map[string]interface{}{
			log.KeySubscriptionID: request.SubscriptionId,
			log.KeyError:          err,
		})
	}
	return &pbtrigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context,
	request *pbtrigger.PauseSubscriptionRequest) (*pbtrigger.PauseSubscriptionResponse, error) {
	log.Info(ctx, "subscription pause ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	s.worker.PauseSubscription(vanus.NewIDFromUint64(request.SubscriptionId))
	return &pbtrigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context,
	request *pbtrigger.ResumeSubscriptionRequest) (*pbtrigger.ResumeSubscriptionResponse, error) {
	log.Info(ctx, "subscription resume ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	return &pbtrigger.ResumeSubscriptionResponse{}, nil
}

func (s *server) Initialize(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	_, err := s.client.registerTriggerWorker(ctx, &controller.RegisterTriggerWorkerRequest{
		Address: s.config.TriggerAddr,
	})
	if err != nil {
		log.Error(ctx, "register trigger worker error", map[string]interface{}{
			"tcAddr":     s.config.ControllerAddr,
			log.KeyError: err,
		})
		s.client.Close(ctx)
		return err
	}
	log.Info(ctx, "trigger worker register success", map[string]interface{}{
		"triggerCtrlAddr":   s.config.ControllerAddr,
		"triggerWorkerAddr": s.config.TriggerAddr,
	})
	s.state = primitive.ServerStateStarted
	s.startTime = time.Now()
	go func() {
		time.Sleep(60 * time.Second)
		// 启动60s后还没有收到start，则退出.
		if s.state != primitive.ServerStateRunning {
			os.Exit(1)
		}
	}()
	return nil
}

func (s *server) Close(ctx context.Context) error {
	log.Info(ctx, "trigger worker server stop...", nil)
	s.stop(true)
	log.Info(ctx, "trigger worker server stopped", nil)
	return nil
}

func (s *server) stop(sendUnregister bool) {
	if s.state != primitive.ServerStateRunning {
		return
	}
	_ = s.worker.Stop()
	s.cancel()
	s.wg.Wait()
	ctx := context.Background()
	if sendUnregister {
		_, err := s.client.unregisterTriggerWorker(ctx, &controller.UnregisterTriggerWorkerRequest{
			Address: s.config.TriggerAddr,
		})
		if err != nil {
			log.Error(ctx, "unregister trigger worker error", map[string]interface{}{
				"addr":       s.config.ControllerAddr,
				log.KeyError: err,
			})
		} else {
			log.Info(ctx, "unregister trigger worker success", nil)
		}
	}
	s.client.Close(ctx)
	s.state = primitive.ServerStateStopped
}

func (s *server) startHeartbeat() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Second * 3)
		defer ticker.Stop()
		ctx := context.Background()
		for {
			select {
			case <-s.ctx.Done():
				s.client.closeHeartBeat(ctx)
				return
			case <-ticker.C:
				workerSub, callback := s.worker.ListSubscriptionInfo()
				var subInfos []*meta.SubscriptionInfo
				for _, sub := range workerSub {
					subInfos = append(subInfos, convert.ToPbSubscriptionInfo(sub))
				}
				err := s.client.heartbeat(ctx, &controller.TriggerWorkerHeartbeatRequest{
					Address:  s.config.TriggerAddr,
					SubInfos: subInfos,
				})
				if err != nil {
					log.Warning(ctx, "send heartbeat to controller failed, connection lost. try to reconnecting", map[string]interface{}{
						log.KeyError: err,
					})
				}
				callback()
			}
		}
	}()
}
