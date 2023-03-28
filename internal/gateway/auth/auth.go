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

package auth

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"github.com/vanus-labs/vanus/internal/primitive/authentication"
	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
)

const TokenType = "Bearer"

type Config struct {
	Disable          bool
	OpenEventbus     bool
	OpenSubscription bool
}

type Auth struct {
	config         Config
	Authentication authentication.Authentication
	Authorization  authorization.Authorization
	authorizeFunc  map[string]AuthorizeFunc
	RoleClient     authorization.RoleClient
	TokenClient    authentication.TokenClient
}

func NewAuth(config Config, cluster cluster.Cluster) *Auth {
	tokenClient := authentication.NewBuiltInClient(cluster)
	roleClient := authorization.NewBuiltInClient(cluster)
	return &Auth{
		config:         config,
		authorizeFunc:  map[string]AuthorizeFunc{},
		TokenClient:    tokenClient,
		RoleClient:     roleClient,
		Authentication: authentication.NewAuthentication(tokenClient),
		Authorization:  authorization.NewAuthorization(roleClient, cluster),
	}
}

func (a *Auth) Disable() bool {
	return a.config.Disable
}

func (a *Auth) OpenEventbus() bool {
	return a.config.OpenEventbus
}

func (a *Auth) OpenSubscription() bool {
	return !a.config.OpenSubscription
}

func (a *Auth) GetRoleClient() authorization.RoleClient {
	return a.RoleClient
}

func (a *Auth) Authenticate(ctx context.Context) (context.Context, error) {
	if a.Disable() {
		return ctx, nil
	}
	token, err := grpc_auth.AuthFromMD(ctx, TokenType)
	if err != nil {
		log.Info(ctx).Err(err).Msg("get authorization token error")
		return ctx, errors.ErrInvalidRequest.WithMessage(err.Error())
	}
	user, err := a.Authentication.Authenticate(ctx, token)
	if err != nil {
		if errors.Is(err, errors.ErrResourceNotFound) {
			log.Debug(ctx).Str("token", token).Msg("authorization token failed")
			return ctx, errors.ErrUnauthenticated.WithMessage("token is invalid")
		}
		log.Info(ctx).Err(err).Str("token", token).Msg("authorization token error")
		return ctx, err
	}
	newCtx := SetUser(ctx, user)
	return newCtx, nil
}

func (a *Auth) Authorize(ctx context.Context, method string, req interface{}) error {
	if a.Disable() {
		return nil
	}
	user := GetUser(ctx)
	authorizeFunc, exist := a.authorizeFunc[method]
	if !exist {
		return nil
	}
	kind, id, action := authorizeFunc(ctx, req)
	if kind == authorization.ResourceUnknown {
		// no need authorize
		return nil
	}
	result, err := a.Authorization.Authorize(ctx, user, authorization.NewDefaultAttributes(kind, id, action))
	if err != nil {
		return err
	}
	if !result {
		log.Info(ctx).
			Str("method", method).
			Str("user", user).
			Interface("req", req).
			Msg("method permission denied")
		return errors.ErrPermissionDenied.WithMessage("no permission")
	}
	return nil
}
