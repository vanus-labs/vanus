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
	"github.com/linkall-labs/vanus/internal/primitive/errors"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"time"
)

type server struct {
	worker       *Worker
	stopCallback func()
	twAddr       string
	tcAddr       string
	tcCc         *grpc.ClientConn
	tcClient     controller.TriggerControllerClient
	cancel       context.CancelFunc
	ctx          context.Context
}

func NewTriggerServer(tcAddr, twAddr string, stop func()) trigger.TriggerWorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &server{
		twAddr:       twAddr,
		tcAddr:       tcAddr,
		stopCallback: stop,
		worker:       NewWorker(),
		cancel:       cancel,
		ctx:          ctx,
	}
}

func (s *server) Start(ctx context.Context, request *trigger.StartTriggerWorkerRequest) (*trigger.StartTriggerWorkerResponse, error) {
	log.Info("worker server start ", map[string]interface{}{"request": request})
	err := s.worker.Start()
	if err != nil {
		return nil, err
	}
	err = s.startHeartbeat()
	if err != nil {
		s.worker.Stop()
		return nil, err
	}
	return &trigger.StartTriggerWorkerResponse{}, nil
}

func (s *server) Stop(ctx context.Context, request *trigger.StopTriggerWorkerRequest) (*trigger.StopTriggerWorkerResponse, error) {
	log.Info("worker server stop ", map[string]interface{}{"request": request})
	s.stop()
	return &trigger.StopTriggerWorkerResponse{}, nil
}

func (s *server) AddSubscription(ctx context.Context, request *trigger.AddSubscriptionRequest) (*trigger.AddSubscriptionResponse, error) {
	log.Info("subscription add ", map[string]interface{}{"request": request})
	sub, err := convert.MetaSubToInnerSub(request.Subscription)
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

func (s *server) Initialize() error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cc, err := grpc.Dial(s.tcAddr, opts...)
	if err != nil {
		return err
	}
	s.tcCc = cc
	s.tcClient = controller.NewTriggerControllerClient(cc)
	_, err = s.tcClient.RegisterTriggerWorker(context.Background(), &controller.RegisterTriggerWorkerRequest{
		Address: s.twAddr,
	})
	if err != nil {
		log.Error("register trigger worker error", map[string]interface{}{
			"tcAddr":     s.tcAddr,
			log.KeyError: err,
		})
		cc.Close()
		return err
	}
	return nil
}

func (s *server) stop() {
	s.worker.Stop()
	s.cancel()
	s.stopCallback()
}
func (s *server) beforeStop() error {
	_, err := s.tcClient.UnregisterTriggerWorker(context.Background(), &controller.UnregisterTriggerWorkerRequest{
		Address: s.twAddr,
	})
	if err != nil {
		log.Error("unregister trigger worker failed", map[string]interface{}{
			"addr":       s.tcAddr,
			log.KeyError: err,
		})
		return err
	}
	log.Info("unregister trigger worker success", nil)
	return nil
}
func (s *server) Close() error {
	s.stop()
	err := s.beforeStop()
	if err != nil {
		return err
	}
	return nil
}

func (s *server) startHeartbeat() error {
	stream, err := s.tcClient.TriggerWorkerHeartbeat(s.ctx)
	if err != nil {
		log.Warning("heartbeat error", map[string]interface{}{"addr": s.tcAddr, "error": err})
		return err
	}
	go func() {
		tk := time.NewTicker(time.Second * 100)
		defer tk.Stop()
	sendLoop:
		for {
			select {
			case <-s.ctx.Done():
				break sendLoop
			case <-tk.C:
				err = stream.Send(&controller.TriggerWorkerHeartbeatRequest{
					Address: s.twAddr,
				})
				if err != nil {
					log.Warning("heartbeat send request error", map[string]interface{}{
						"addr":       s.tcAddr,
						log.KeyError: err,
					})
				}
			}
		}
	}()

	return nil
}
