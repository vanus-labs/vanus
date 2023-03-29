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

package cluster

import (
	"context"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

type authService struct {
	client ctrlpb.AuthControllerClient
}

func (a *authService) GetUserRole(ctx context.Context, user string) ([]*metapb.UserRole, error) {
	resp, err := a.client.GetUserRole(ctx, &ctrlpb.GetUserRoleRequest{UserIdentifier: user})
	if err != nil {
		return nil, errors.UnwrapOrUnknown(err)
	}
	return resp.GetUserRole(), nil
}

func (a *authService) GetUserByToken(ctx context.Context, token string) (string, error) {
	user, err := a.client.GetUserByToken(ctx, wrapperspb.String(token))
	if err != nil {
		return "", errors.UnwrapOrUnknown(err)
	}
	return user.GetValue(), nil
}

func (a *authService) RawClient() ctrlpb.AuthControllerClient {
	return a.client
}

func newAuthService(cc *raw_client.Conn) AuthService {
	return &authService{
		client: raw_client.NewAuthClient(cc),
	}
}
