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

package credentials

import (
	// standard libraries.
	"context"

	// third-party libraries.
	"golang.org/x/oauth2"
)

type VanusPerRPCCredentials struct {
	oauth2.TokenSource
}

func (vc VanusPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := vc.Token()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": token.Type() + " " + token.AccessToken,
	}, nil
}

func (vc VanusPerRPCCredentials) RequireTransportSecurity() bool {
	return false
}

func NewVanusPerRPCCredentials(token string) VanusPerRPCCredentials {
	return VanusPerRPCCredentials{
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: token,
		}),
	}
}
