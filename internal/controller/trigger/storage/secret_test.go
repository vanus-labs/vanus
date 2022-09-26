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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/pkg/util/crypto"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSecretStorage(t *testing.T) {
	Convey("test secret", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		kvClient := kv.NewMockClient(ctrl)
		p, err := NewSecretStorage(primitive.KvStorageConfig{
			ServerList: []string{"test"},
		}, "just_for_test")
		So(err, ShouldBeNil)
		secret := p.(*SecretStorage)
		secret.client = kvClient
		Convey("test credential type AK/SK", func() {
			subID := vanus.NewID()
			Convey("test read", func() {
				a, _ := crypto.AESEncrypt("test_access_key_id", secret.cipherKey)
				s, _ := crypto.AESEncrypt("test_secret_access_key", secret.cipherKey)
				v, _ := json.Marshal(primitive.NewAkSkSinkCredential(a, s))
				kvClient.EXPECT().Get(ctx, secret.getKey(subID)).Return(v, nil)
				credential, err := secret.Read(ctx, subID, primitive.AWS)
				So(err, ShouldBeNil)
				So(credential.GetType(), ShouldEqual, primitive.AWS)
				So(credential.(*primitive.AkSkSinkCredential).AccessKeyID, ShouldEqual, "test_access_key_id")
				So(credential.(*primitive.AkSkSinkCredential).SecretAccessKey, ShouldEqual, "test_secret_access_key")
			})
			Convey("test write", func() {
				credential := primitive.NewAkSkSinkCredential("test_access_key_id", "test_secret_access_key")
				kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
				err := secret.Write(ctx, subID, credential)
				So(err, ShouldBeNil)
			})
		})
		Convey("test credential type gcloud", func() {
			subID := vanus.NewID()
			Convey("test read", func() {
				a, _ := crypto.AESEncrypt("{\"type\":\"service_account\"}", secret.cipherKey)
				v, _ := json.Marshal(primitive.NewGCloudSinkCredential(a))
				kvClient.EXPECT().Get(ctx, secret.getKey(subID)).Return(v, nil)
				credential, err := secret.Read(ctx, subID, primitive.GCloud)
				So(err, ShouldBeNil)
				So(credential.GetType(), ShouldEqual, primitive.GCloud)
				So(credential.(*primitive.GCloudSinkCredential).CredentialJSON, ShouldEqual, "{\"type\":\"service_account\"}")
			})
			Convey("test write", func() {
				credential := primitive.NewGCloudSinkCredential("{\"type\":\"service_account\"}")
				kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
				err := secret.Write(ctx, subID, credential)
				So(err, ShouldBeNil)
			})
		})
		Convey("test credential type plain", func() {
			subID := vanus.NewID()
			Convey("test read", func() {
				a, _ := crypto.AESEncrypt("test_identifier", secret.cipherKey)
				s, _ := crypto.AESEncrypt("test_secret", secret.cipherKey)
				v, _ := json.Marshal(primitive.NewPlainSinkCredential(a, s))
				kvClient.EXPECT().Get(ctx, secret.getKey(subID)).Return(v, nil)
				credential, err := secret.Read(ctx, subID, primitive.Plain)
				So(err, ShouldBeNil)
				So(credential.GetType(), ShouldEqual, primitive.Plain)
				So(credential.(*primitive.PlainSinkCredential).Identifier, ShouldEqual, "test_identifier")
				So(credential.(*primitive.PlainSinkCredential).Secret, ShouldEqual, "test_secret")
			})
			Convey("test write", func() {
				credential := primitive.NewPlainSinkCredential("test_identifier", "test_secret")
				kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
				err := secret.Write(ctx, subID, credential)
				So(err, ShouldBeNil)
			})
		})
		Convey("test delete", func() {
			subID := vanus.NewID()
			kvClient.EXPECT().Delete(ctx, secret.getKey(subID)).Return(nil)
			err := secret.Delete(ctx, subID)
			So(err, ShouldBeNil)
		})
	})
}
