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
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/trigger/worker"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	pbtrigger "github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"sync"
	"time"
)

type server struct {
	worker   *worker.Worker
	config   Config
	tcCc     *grpc.ClientConn
	tcClient controller.TriggerControllerClient
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	state    primitive.ServerState
}

func NewTriggerServer(config Config) pbtrigger.TriggerWorkerServer {
	s := &server{
		config: config,
		worker: worker.NewWorker(),
		state:  primitive.ServerStateCreated,
	}
	return s
}

func (s *server) Start(ctx context.Context, request *pbtrigger.StartTriggerWorkerRequest) (*pbtrigger.StartTriggerWorkerResponse, error) {
	log.Info(ctx, "worker server start ", map[string]interface{}{"request": request})
	err := s.worker.Start()
	if err != nil {
		return nil, err
	}
	if s.state == primitive.ServerStateRunning {
		return &pbtrigger.StartTriggerWorkerResponse{}, nil
	}
	s.startHeartbeat()
	s.state = primitive.ServerStateRunning
	return &pbtrigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context, request *pbtrigger.StopTriggerWorkerRequest) (*pbtrigger.StopTriggerWorkerResponse, error) {
	log.Info(ctx, "worker server stop ", map[string]interface{}{"request": request})
	s.stop(false)
	os.Exit(1)
	return &pbtrigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context, request *pbtrigger.AddSubscriptionRequest) (*pbtrigger.AddSubscriptionResponse, error) {
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

func (s *server) RemoveSubscription(ctx context.Context, request *pbtrigger.RemoveSubscriptionRequest) (*pbtrigger.RemoveSubscriptionResponse, error) {
	log.Info(ctx, "subscription remove ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	err := s.worker.RemoveSubscription(request.Id)
	if err != nil {
		log.Info(ctx, "remove subscription error", map[string]interface{}{
			"id":         request.Id,
			log.KeyError: err,
		})
	}
	return &pbtrigger.RemoveSubscriptionResponse{}, nil
}

func (s *server) PauseSubscription(ctx context.Context, request *pbtrigger.PauseSubscriptionRequest) (*pbtrigger.PauseSubscriptionResponse, error) {
	log.Info(ctx, "subscription pause ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	s.worker.PauseSubscription(request.Id)
	return &pbtrigger.PauseSubscriptionResponse{}, nil
}

func (s *server) ResumeSubscription(ctx context.Context, request *pbtrigger.ResumeSubscriptionRequest) (*pbtrigger.ResumeSubscriptionResponse, error) {
	log.Info(ctx, "subscription resume ", map[string]interface{}{"request": request})
	if s.state != primitive.ServerStateRunning {
		return nil, errors.ErrWorkerNotStart
	}
	return &pbtrigger.ResumeSubscriptionResponse{}, nil
}

func (s *server) init(ctx context.Context) error {
	if s.tcCc != nil {
		return nil
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	timeout, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	cc, err := grpc.DialContext(timeout, s.config.TriggerCtrlAddr, opts...)
	if err != nil {
		return err
	}
	s.tcCc = cc
	s.tcClient = controller.NewTriggerControllerClient(cc)
	return nil
}

func (s *server) Initialize(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	err := s.init(s.ctx)
	if err != nil {
		return err
	}
	_, err = s.tcClient.RegisterTriggerWorker(ctx, &controller.RegisterTriggerWorkerRequest{
		Address: s.config.TriggerAddr,
	})
	if err != nil {
		log.Error(ctx, "register trigger worker error", map[string]interface{}{
			"tcAddr":     s.config.TriggerCtrlAddr,
			log.KeyError: err,
		})
		s.tcCc.Close()
		return err
	}
	log.Info(ctx, "trigger worker register success", map[string]interface{}{
		"triggerCtrlAddr":   s.config.TriggerCtrlAddr,
		"triggerWorkerAddr": s.config.TriggerAddr,
	})
	go func() {
		<-ctx.Done()
		log.Info(ctx, "trigger worker server stop...", nil)
		s.stop(true)
		log.Info(ctx, "trigger worker server stopped", nil)
	}()
	s.state = primitive.ServerStateStarted
	return nil
}

func (s *server) stop(sendUnregister bool) {
	if s.state == primitive.ServerStateStopped {
		return
	}
	s.cancel()
	s.worker.Stop()
	if sendUnregister {
		_, err := s.tcClient.UnregisterTriggerWorker(context.Background(), &controller.UnregisterTriggerWorkerRequest{
			Address: s.config.TriggerAddr,
		})
		if err != nil {
			log.Error(s.ctx, "unregister trigger worker error", map[string]interface{}{
				"addr":       s.config.TriggerCtrlAddr,
				log.KeyError: err,
			})
		} else {
			log.Info(s.ctx, "unregister trigger worker success", nil)
		}
	}
	s.wg.Wait()
	s.tcCc.Close()
	s.state = primitive.ServerStateStopped
}

const (
	heartbeatMaxConnTime = 5 * time.Minute
)

func (s *server) initHeartbeat(ctx context.Context) (controller.TriggerController_TriggerWorkerHeartbeatClient, error) {
	err := s.init(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := s.tcClient.TriggerWorkerHeartbeat(s.ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *server) startHeartbeat() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		beginConnTime := time.Now()
		lastSendTime := time.Now()
		for {
			stream, err := s.initHeartbeat(s.ctx)
			if err != nil {
				switch err {
				case context.Canceled:
					return
				default:
					log.Warning(s.ctx, "heartbeat error", map[string]interface{}{
						"addr":       s.config.TriggerCtrlAddr,
						log.KeyError: err,
					})
					if time.Now().Sub(beginConnTime) > heartbeatMaxConnTime {
						log.Error(s.ctx, "heartbeat exit", map[string]interface{}{
							"addr":       s.config.TriggerCtrlAddr,
							log.KeyError: err,
						})
						//todo now exit trigger worker, maybe only stop trigger
						os.Exit(1)
					}
					if !util.SleepWithContext(s.ctx, time.Second*2) {
						return
					}
					continue
				}

			}
		sendLoop:
			for {
				workerSub, callback := s.worker.ListSubInfos()
				var subInfos []*meta.SubscriptionInfo
				for _, sub := range workerSub {
					subInfos = append(subInfos, convert.ToPbSubscriptionInfo(sub))
				}
				err = stream.Send(&controller.TriggerWorkerHeartbeatRequest{
					Address:  s.config.TriggerAddr,
					SubInfos: subInfos,
				})
				if err != nil {
					if err == io.EOF || time.Now().Sub(lastSendTime) > 5*time.Second {
						stream.CloseSend()
						log.Warning(s.ctx, "heartbeat send request receive fail,will retry", map[string]interface{}{
							"addr": s.config.TriggerCtrlAddr,
						})
						beginConnTime = time.Now()
						break sendLoop
					}
					log.Warning(s.ctx, "heartbeat send request error", map[string]interface{}{
						"addr":       s.config.TriggerCtrlAddr,
						log.KeyError: err,
					})
				} else {
					callback()
					lastSendTime = time.Now()
				}
				if !util.SleepWithContext(s.ctx, time.Second) {
					stream.CloseSend()
					return
				}
			}
		}
	}()
}
