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

package utils

import (
	"context"
	"os"

	"github.com/linkall-labs/sdk/proto/pkg/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewClient(ctx context.Context, endpoint string) vanus.ClientClient {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		log.Error(ctx, "failed to connect to gateway", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(1)
	}
	return vanus.NewClientClient(conn)
}
