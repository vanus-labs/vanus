// Copyright 2023 Linkall Inc.
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

package authentication

import (
	"context"
	"sync"
	"time"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/errors"
)

//go:generate mockgen -source=authentication.go -destination=mock_authentication.go -package=authentication
type Authentication interface {
	// Authenticate check token valid and return user identifier
	Authenticate(ctx context.Context, token string) (string, error)
}

var _ Authentication = &authentication{}

const checkExpireTime = 30 * time.Second

type authentication struct {
	client     TokenClient
	tokens     sync.Map
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewAuthentication(client TokenClient) Authentication {
	return &authentication{
		client: client,
	}
}

func (a *authentication) Start(ctx context.Context) error {
	a.ctx, a.cancelFunc = context.WithCancel(ctx)
	go a.checkTokenExpired()
	return nil
}

func (a *authentication) Stop(ctx context.Context) error { //nolint:revive // ignore
	a.cancelFunc()
	return nil
}

func (a *authentication) checkTokenExpired() {
	ticker := time.NewTicker(checkExpireTime)
	defer ticker.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.tokens.Range(func(key, value any) bool {
				token, _ := key.(string)
				v, err := a.client.GetUser(a.ctx, token)
				if err != nil {
					log.Warn(a.ctx).Msg("get token has error")
					return true
				}
				if v == "" {
					a.tokens.Delete(token)
				}
				return true
			})
		}
	}
}

func (a *authentication) Authenticate(ctx context.Context, token string) (string, error) {
	v, exist := a.tokens.Load(token)
	if exist {
		return v.(string), nil
	}
	// todo breakdown cache
	user, err := a.client.GetUser(ctx, token)
	if err != nil {
		return "", err
	}
	if user == "" {
		return "", errors.ErrResourceNotFound.WithMessage("token is invalid")
	}
	a.tokens.Store(token, user)
	return user, nil
}
