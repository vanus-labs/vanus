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

//go:generate mockgen -source=persistence.go  -destination=mock_persistence.go -package=secret
package secret

import (
	"context"
	"encoding/json"
	"path"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/util/crypto"
)

type Persistence interface {
	Read(ctx context.Context, subID vanus.ID) (primitive.SinkCredential, error)
	Write(ctx context.Context, subID vanus.ID, credential primitive.SinkCredential) error
	Delete(ctx context.Context, subID vanus.ID) error
}

func NewEtcdPersistence(config primitive.KvStorageConfig, encryption string) (Persistence, error) {
	client, err := etcd.NewEtcdClientV3(config.ServerList, config.KeyPrefix)
	if err != nil {
		return nil, err
	}
	return &etcdPersistence{
		client:    client,
		cipherKey: encryption,
	}, nil
}

type etcdPersistence struct {
	client    kv.Client
	cipherKey string
}

func (p *etcdPersistence) getKey(subID vanus.ID) string {
	return path.Join(storage.KeyPrefixSecret.String(), subID.String())
}

func (p *etcdPersistence) Read(ctx context.Context, subID vanus.ID) (primitive.SinkCredential, error) {
	credential := primitive.SinkCredential{}
	if v, err := p.client.Get(ctx, p.getKey(subID)); err != nil {
		return credential, err
	} else if err = json.Unmarshal(v, &credential); err != nil {
		return credential, errors.ErrJSONUnMarshal.Wrap(err)
	}
	switch credential.Type {
	case primitive.CloudCredentialType:
		v, err := crypto.AesDecrypt(credential.AccessKeyID, p.cipherKey)
		if err != nil {
			return credential, errors.ErrAesCrypto.Wrap(err)
		}
		credential.AccessKeyID = v
		v, err = crypto.AesDecrypt(credential.SecretAccessKey, p.cipherKey)
		if err != nil {
			return credential, errors.ErrAesCrypto.Wrap(err)
		}
		credential.SecretAccessKey = v
	case primitive.PlainCredentialType:
		v, err := crypto.AesDecrypt(credential.Identifier, p.cipherKey)
		if err != nil {
			return credential, errors.ErrAesCrypto.Wrap(err)
		}
		credential.Identifier = v
		v, err = crypto.AesDecrypt(credential.Secret, p.cipherKey)
		if err != nil {
			return credential, errors.ErrAesCrypto.Wrap(err)
		}
		credential.Secret = v
	}
	return credential, nil
}

func (p *etcdPersistence) Write(ctx context.Context, subID vanus.ID, credential primitive.SinkCredential) error {
	save := &primitive.SinkCredential{
		Type: credential.Type,
	}
	switch credential.Type {
	case primitive.CloudCredentialType:
		v, err := crypto.AesEncrypt(credential.AccessKeyID, p.cipherKey)
		if err != nil {
			return errors.ErrAesCrypto.Wrap(err)
		}
		save.AccessKeyID = v
		v, err = crypto.AesEncrypt(credential.SecretAccessKey, p.cipherKey)
		if err != nil {
			return errors.ErrAesCrypto.Wrap(err)
		}
		save.SecretAccessKey = v
	case primitive.PlainCredentialType:
		v, err := crypto.AesEncrypt(credential.Identifier, p.cipherKey)
		if err != nil {
			return errors.ErrAesCrypto.Wrap(err)
		}
		save.Identifier = v
		v, err = crypto.AesEncrypt(credential.Secret, p.cipherKey)
		if err != nil {
			return errors.ErrAesCrypto.Wrap(err)
		}
		save.Secret = v
	}

	v, err := json.Marshal(save)
	if err != nil {
		return errors.ErrJSONMarshal.Wrap(err)
	}
	key := p.getKey(subID)
	exist, err := p.client.Exists(ctx, key)
	if err != nil {
		return err
	}
	if !exist {
		return p.client.Create(ctx, key, v)
	}
	return p.client.Update(ctx, key, v)
}

func (p *etcdPersistence) Delete(ctx context.Context, subID vanus.ID) error {
	key := p.getKey(subID)
	return p.client.Delete(ctx, key)
}
