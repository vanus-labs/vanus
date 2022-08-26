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

//go:generate mockgen -source=storage.go  -destination=mock_storage.go -package=secret
package secret

import (
	"context"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

type Storage interface {
	Read(ctx context.Context, subID vanus.ID, credentialType primitive.CredentialType) (primitive.SinkCredential, error)
	Write(ctx context.Context, subID vanus.ID, credential primitive.SinkCredential) error
	Delete(ctx context.Context, subID vanus.ID) error
}
