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

	"github.com/vanus-labs/vanus/api/cluster"
)

//go:generate mockgen -source=client.go -destination=mock_client.go -package=authentication
type TokenClient interface {
	GetUser(ctx context.Context, token string) (string, error)
}

var _ TokenClient = &builtInClient{}

type builtInClient struct {
	cluster cluster.Cluster
}

func NewBuiltInClient(cluster cluster.Cluster) TokenClient {
	return &builtInClient{
		cluster: cluster,
	}
}

func (c *builtInClient) GetUser(ctx context.Context, token string) (string, error) {
	return c.cluster.AuthService().GetUserByToken(ctx, token)
}
