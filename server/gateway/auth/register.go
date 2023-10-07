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
	"fmt"

	vanus "github.com/vanus-labs/vanus/api/vsr"

	"github.com/vanus-labs/vanus/pkg/authorization"
)

type AuthorizeFunc func(ctx context.Context,
	req interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action)

func (a *Auth) RegisterAuthorizeFunc(method string, authorizeFunc AuthorizeFunc) {
	if authorizeFunc == nil {
		panic(fmt.Sprintf("method %s authorize function is nil", method))
	}
	_, exist := a.authorizeFunc[method]
	if exist {
		panic(fmt.Sprintf("method %s exist authorize function", method))
	}
	a.authorizeFunc[method] = authorizeFunc
}
