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
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
	"sync"
	"time"
)

type server struct {
	worker       *Worker
	stopCallback func()
	twAddr       string
	tcAddr       string
	tcCc         *grpc.ClientConn
	tcClient     controller.TriggerControllerClient
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewTriggerServer(tcAddr, twAddr string, config Config, stop func()) trigger.TriggerWorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &server{
		twAddr:       twAddr,
		tcAddr:       tcAddr,
		stopCallback: stop,
		worker:       NewWorker(config),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *server) Start(ctx context.Context, request *trigger.StartTriggerWorkerRequest) (*trigger.StartTriggerWorkerResponse, error) {
	log.Info("worker server start ", map[string]interface{}{"request": request})
	err := s.worker.Start()
	if err != nil {
		return nil, err
	}
	s.startHeartbeat()
	return &trigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context, request *trigger.StopTriggerWorkerRequest) (*trigger.StopTriggerWorkerResponse, error) {
	log.Info("worker server stop ", map[string]interface{}{"request": request})
	s.stop()
	s.tcCc.Close()
	return &trigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context, request *trigger.AddSubscriptionRequest) (*trigger.AddSubscriptionResponse, error) {
	log.Info("subscription add ", map[string]interface{}{"request": request})
	sub, err := convert.FromPbSubscription(request.Subscription)
	if err != nil {
		log.Info("trigger subscription request to subscription error", map[string]interface{}{"error": err})
		return nil, err
	}
	err = s.worker.AddSubscription(sub)
	if err != nil {
		if err == SubExist {
			log.Info("add subscription bus sub exist", map[string]interface{}{
				"id": sub.ID,
			})
		} else {
			log.Warning("worker add subscription error ", map[string]interface{}{"subscription": sub, "error": err})
			return nil, err
		}
	}
	return &trigger.AddSubscriptionResponse{}, nil
}

func (s *server) RemoveSubscription(ctx context.Context, request *trigger.RemoveSubscriptionRequest) (*trigger.RemoveSubscriptionResponse, error) {
	log.Info("subscription remove ", map[string]interface{}{"request": request})
	err := s.worker.RemoveSubscription(request.Id)
	if err != nil {
		log.Info("remove subscription error", map[string]interface{}{
			"id":         request.Id,
			log.KeyError: err,
		})
	}
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

func (s *server) Initialize(ctx context.Context) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cc, err := grpc.Dial(s.tcAddr, opts...)
	if err != nil {
		return err
	}
	s.tcCc = cc
	s.tcClient = controller.NewTriggerControllerClient(cc)
	_, err = s.tcClient.RegisterTriggerWorker(ctx, &controller.RegisterTriggerWorkerRequest{
		Address: s.twAddr,
	})
	if err != nil {
		log.Error("register trigger worker error", map[string]interface{}{
			"tcAddr":     s.tcAddr,
			log.KeyError: err,
		})
		s.tcCc.Close()
		return err
	}
	return nil
}

func (s *server) stop() {
	s.worker.Stop()
	s.cancel()
	s.wg.Wait()
}

func (s *server) Close() error {
	s.stopCallback()
	s.stop()
	_, err := s.tcClient.UnregisterTriggerWorker(context.Background(), &controller.UnregisterTriggerWorkerRequest{
		Address: s.twAddr,
	})
	if err != nil {
		log.Error("unregister trigger worker failed", map[string]interface{}{
			"addr":       s.tcAddr,
			log.KeyError: err,
		})
	} else {
		log.Info("unregister trigger worker success", nil)
	}
	return s.tcCc.Close()
}

const (
	heartbeatMaxConnTime = 5 * time.Minute
)

func (s *server) startHeartbeat() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var stream controller.TriggerController_TriggerWorkerHeartbeatClient
		var err error
		beginConnTime := time.Now()
		lastSendTime := time.Now()
		for {
			stream, err = s.tcClient.TriggerWorkerHeartbeat(context.Background())
			if err != nil {
				log.Info("heartbeat error", map[string]interface{}{
					"addr":       s.tcAddr,
					log.KeyError: err,
				})
				if time.Now().Sub(beginConnTime) > heartbeatMaxConnTime {
					log.Error("heartbeat exit", map[string]interface{}{
						"addr":       s.tcAddr,
						log.KeyError: err,
					})
					//todo now exit trigger worker, maybe only stop trigger
					os.Exit(1)
				}
				if !util.Sleep(s.ctx, time.Second*2) {
					return
				}
				continue
			}
		sendLoop:
			for {
				subs := s.worker.ListSubscription()
				var ids []string
				for _, sub := range subs {
					ids = append(ids, sub.ID)
				}
				err = stream.Send(&controller.TriggerWorkerHeartbeatRequest{
					Started: s.worker.started,
					Address: s.twAddr,
					SubIds:  ids,
				})
				if err != nil {
					if err == io.EOF || time.Now().Sub(lastSendTime) > 5*time.Second {
						stream.CloseSend()
						log.Warning("heartbeat send request receive fail,will retry", map[string]interface{}{
							"time": time.Now(),
							"addr": s.tcAddr,
						})
						beginConnTime = time.Now()
						break sendLoop
					}
					log.Info("heartbeat send request error", map[string]interface{}{
						"time":       time.Now(),
						"addr":       s.tcAddr,
						log.KeyError: err,
					})
				} else {
					lastSendTime = time.Now()
				}
				if !util.Sleep(s.ctx, time.Second) {
					stream.CloseSend()
					return
				}
			}
		}
	}()
}
