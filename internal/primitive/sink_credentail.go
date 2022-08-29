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

func FillSinkCredential(dst, src SinkCredential) {
	if dst == nil || src == nil {
		return
	}
	if dst.GetType() != src.GetType() {
		return
	}
	switch dst.GetType() {
	case Plain:
		_dst, _ := dst.(*PlainSinkCredential)
		_src, _ := src.(*PlainSinkCredential)
		if _dst.Identifier == SecretsMask {
			_dst.Identifier = _src.Identifier
		}
		if _dst.Secret == SecretsMask {
			_dst.Secret = _src.Secret
		}
	case Cloud:
		_dst, _ := src.(*CloudSinkCredential)
		_src, _ := dst.(*CloudSinkCredential)
		if _dst.AccessKeyID == SecretsMask {
			_dst.AccessKeyID = _src.AccessKeyID
		}
		if _dst.SecretAccessKey == SecretsMask {
			_dst.SecretAccessKey = _src.SecretAccessKey
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
