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
	Plain  CredentialType = "plain"
	AkSk   CredentialType = "ak_sk"
	GCloud CredentialType = "gcloud"

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
	case AkSk:
		_dst, _ := src.(*AkSkSinkCredential)
		_src, _ := dst.(*AkSkSinkCredential)
		if _dst.AccessKeyID == SecretsMask {
			_dst.AccessKeyID = _src.AccessKeyID
		}
		if _dst.SecretAccessKey == SecretsMask {
			_dst.SecretAccessKey = _src.SecretAccessKey
		}
	case GCloud:
		_dst, _ := src.(*GCloudSinkCredential)
		_src, _ := dst.(*GCloudSinkCredential)
		if _dst.CredentialJSON == SecretsMask {
			_dst.CredentialJSON = _src.CredentialJSON
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

type AkSkSinkCredential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func NewAkSkSinkCredential(accessKeyID, secretAccessKey string) SinkCredential {
	return &AkSkSinkCredential{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}
}

func (c *AkSkSinkCredential) GetType() CredentialType {
	return AkSk
}

type GCloudSinkCredential struct {
	CredentialJSON string `json:"credential_json"`
}

func NewGCloudSinkCredential(credentialJSON string) SinkCredential {
	return &GCloudSinkCredential{
		CredentialJSON: credentialJSON,
	}
}

func (c *GCloudSinkCredential) GetType() CredentialType {
	return GCloud
}
