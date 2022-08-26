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

package primitive

type CredentialType string

const (
	Plain CredentialType = "plain"
	Cloud CredentialType = "cloud"

	SecretsMask = "******"
)

type SinkCredential interface {
	GetType() CredentialType
}

func UpdateSinkCredential(original, now SinkCredential) {
	if original == nil || now == nil {
		return
	}
	if original.GetType() != now.GetType() {
		return
	}
	switch original.GetType() {
	case Plain:
		credential, _ := now.(*PlainSinkCredential)
		_credential, _ := original.(*PlainSinkCredential)
		if credential.Identifier == SecretsMask &&
			credential.Secret == SecretsMask {
			credential.Identifier = _credential.Identifier
			credential.Secret = _credential.Secret
			return
		}
	case Cloud:
		credential, _ := now.(*CloudSinkCredential)
		_credential, _ := original.(*CloudSinkCredential)
		if credential.AccessKeyID == SecretsMask &&
			credential.SecretAccessKey == SecretsMask {
			credential.AccessKeyID = _credential.AccessKeyID
			credential.SecretAccessKey = _credential.SecretAccessKey
		}
	}
}

type PlainSinkCredential struct {
	Identifier string `json:"identifier"`
	Secret     string `json:"secret"`
}

func NewPlainSinkCredential(identifier, secret string) SinkCredential {
	return &PlainSinkCredential{
		Identifier: identifier,
		Secret:     secret,
	}
}

func (c *PlainSinkCredential) GetType() CredentialType {
	return Plain
}

type CloudSinkCredential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func NewCloudSinkCredential(accessKeyID, secretAccessKey string) SinkCredential {
	return &CloudSinkCredential{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
}

func (c *CloudSinkCredential) GetType() CredentialType {
	return Cloud
}
