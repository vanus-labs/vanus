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
		Convey("test credential type cloud", func() {
			subID := vanus.NewID()
			Convey("test read", func() {
				a, _ := crypto.AESEncrypt("test_access_key_id", secret.cipherKey)
				s, _ := crypto.AESEncrypt("test_secret_access_key", secret.cipherKey)
				v, _ := json.Marshal(primitive.NewCloudSinkCredential(a, s))
				kvClient.EXPECT().Get(ctx, secret.getKey(subID)).Return(v, nil)
				credential, err := secret.Read(ctx, subID, primitive.Cloud)
				So(err, ShouldBeNil)
				So(credential.GetType(), ShouldEqual, primitive.Cloud)
				So(credential.(*primitive.CloudSinkCredential).AccessKeyID, ShouldEqual, "test_access_key_id")
				So(credential.(*primitive.CloudSinkCredential).SecretAccessKey, ShouldEqual, "test_secret_access_key")
			})
			Convey("test write", func() {
				credential := primitive.NewCloudSinkCredential("test_access_key_id", "test_secret_access_key")
				Convey("test create", func() {
					kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
					err := secret.Write(ctx, subID, credential)
					So(err, ShouldBeNil)
				})
				Convey("test update", func() {
					kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
					err := secret.Write(ctx, subID, credential)
					So(err, ShouldBeNil)
				})
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
				Convey("test create", func() {
					kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
					err := secret.Write(ctx, subID, credential)
					So(err, ShouldBeNil)
				})
				Convey("test update", func() {
					kvClient.EXPECT().Set(ctx, secret.getKey(subID), gomock.Any()).Return(nil)
					err := secret.Write(ctx, subID, credential)
					So(err, ShouldBeNil)
				})
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
