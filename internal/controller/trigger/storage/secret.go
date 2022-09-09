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

package storage

import (
	"context"
	"encoding/json"
	"path"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/secret"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/kv/etcd"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/pkg/util/crypto"
)

func NewSecretStorage(config primitive.KvStorageConfig, encryption string) (secret.Storage, error) {
	client, err := etcd.NewEtcdClientV3(config.ServerList, config.KeyPrefix)
	if err != nil {
		return nil, err
	}
	return &SecretStorage{
		client:    client,
		cipherKey: encryption,
	}, nil
}

type SecretStorage struct {
	client    kv.Client
	cipherKey string
}

func (p *SecretStorage) getKey(subID vanus.ID) string {
	return path.Join(KeyPrefixSecret.String(), subID.String())
}

func (p *SecretStorage) Read(ctx context.Context,
	subID vanus.ID,
	credentialType primitive.CredentialType) (primitive.SinkCredential, error) {
	v, err := p.client.Get(ctx, p.getKey(subID))
	if err != nil {
		return nil, err
	}
	switch credentialType {
	case primitive.Cloud:
		credential := &primitive.CloudSinkCredential{}
		if err = json.Unmarshal(v, credential); err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}
		accessKeyID, err := crypto.AESDecrypt(credential.AccessKeyID, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		credential.AccessKeyID = accessKeyID
		secretAccessKey, err := crypto.AESDecrypt(credential.SecretAccessKey, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		credential.SecretAccessKey = secretAccessKey
		return credential, nil
	case primitive.Plain:
		credential := &primitive.PlainSinkCredential{}
		if err = json.Unmarshal(v, credential); err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}
		identifier, err := crypto.AESDecrypt(credential.Identifier, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		credential.Identifier = identifier
		s, err := crypto.AESDecrypt(credential.Secret, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		credential.Secret = s
		return credential, nil
	}
	return nil, errors.ErrInvalidRequest.WithMessage("unknown credential type")
}

func (p *SecretStorage) Write(ctx context.Context, subID vanus.ID, credential primitive.SinkCredential) error {
	var save primitive.SinkCredential
	switch credential.GetType() {
	case primitive.Cloud:
		cloud, _ := credential.(*primitive.CloudSinkCredential)
		accessKeyID, err := crypto.AESEncrypt(cloud.AccessKeyID, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		secretAccessKey, err := crypto.AESEncrypt(cloud.SecretAccessKey, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		save = primitive.NewCloudSinkCredential(accessKeyID, secretAccessKey)
	case primitive.Plain:
		plain, _ := credential.(*primitive.PlainSinkCredential)
		identifier, err := crypto.AESEncrypt(plain.Identifier, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		s, err := crypto.AESEncrypt(plain.Secret, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		save = primitive.NewPlainSinkCredential(identifier, s)
	}

	v, err := json.Marshal(save)
	if err != nil {
		return errors.ErrJSONMarshal.Wrap(err)
	}
	key := p.getKey(subID)
	return p.client.Set(ctx, key, v)
}

func (p *SecretStorage) Delete(ctx context.Context, subID vanus.ID) error {
	key := p.getKey(subID)
	return p.client.Delete(ctx, key)
}
