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

	"github.com/vanus-labs/vanus/api/errors"
	vanus "github.com/vanus-labs/vanus/api/vsr"

	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/crypto/aes"
	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/pkg/kv/etcd"
	"github.com/vanus-labs/vanus/server/controller/trigger/secret"
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
	return path.Join(kv.MetadataSecret, subID.Key())
}

func (p *SecretStorage) Read(ctx context.Context,
	subID vanus.ID,
	credentialType primitive.CredentialType,
) (primitive.SinkCredential, error) {
	v, err := p.client.Get(ctx, p.getKey(subID))
	if err != nil {
		return nil, err
	}
	switch credentialType {
	case primitive.AWS:
		credential := &primitive.AkSkSinkCredential{}
		if err = json.Unmarshal(v, credential); err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}
		accessKeyID, err := aes.Decrypt(credential.AccessKeyID, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		secretAccessKey, err := aes.Decrypt(credential.SecretAccessKey, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		return primitive.NewAkSkSinkCredential(accessKeyID, secretAccessKey), nil
	case primitive.GCloud:
		credential := &primitive.GCloudSinkCredential{}
		if err = json.Unmarshal(v, credential); err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}

		credentialJSON, err := aes.Decrypt(credential.CredentialJSON, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		return primitive.NewGCloudSinkCredential(credentialJSON), nil
	case primitive.Plain:
		credential := &primitive.PlainSinkCredential{}
		if err = json.Unmarshal(v, credential); err != nil {
			return nil, errors.ErrJSONUnMarshal.Wrap(err)
		}
		identifier, err := aes.Decrypt(credential.Identifier, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		secret, err := aes.Decrypt(credential.Secret, p.cipherKey)
		if err != nil {
			return nil, errors.ErrAESDecrypt.Wrap(err)
		}
		return primitive.NewPlainSinkCredential(identifier, secret), nil
	}
	return nil, errors.ErrInvalidRequest.WithMessage("unknown credential type")
}

func (p *SecretStorage) Write(ctx context.Context, subID vanus.ID, credential primitive.SinkCredential) error {
	var save primitive.SinkCredential
	switch credential.GetType() {
	case primitive.AWS:
		cloud, _ := credential.(*primitive.AkSkSinkCredential)
		accessKeyID, err := aes.Encrypt(cloud.AccessKeyID, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		secretAccessKey, err := aes.Encrypt(cloud.SecretAccessKey, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		save = primitive.NewAkSkSinkCredential(accessKeyID, secretAccessKey)
	case primitive.GCloud:
		gcloud, _ := credential.(*primitive.GCloudSinkCredential)
		credentialJSON, err := aes.Encrypt(gcloud.CredentialJSON, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		save = primitive.NewGCloudSinkCredential(credentialJSON)
	case primitive.Plain:
		plain, _ := credential.(*primitive.PlainSinkCredential)
		identifier, err := aes.Encrypt(plain.Identifier, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		s, err := aes.Encrypt(plain.Secret, p.cipherKey)
		if err != nil {
			return errors.ErrAESEncrypt.Wrap(err)
		}
		save = primitive.NewPlainSinkCredential(identifier, s)
	default:
		return errors.ErrInvalidRequest.WithMessage("unknown credential type")
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
