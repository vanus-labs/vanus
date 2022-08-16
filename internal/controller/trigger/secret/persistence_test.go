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

package secret

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/util/crypto"
	. "github.com/smartystreets/goconvey/convey"
)

func TestEtcdPersistence(t *testing.T) {
	Convey("test secret", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		kvClient := kv.NewMockClient(ctrl)
		p, err := NewEtcdPersistence(primitive.KvStorageConfig{
			ServerList: []string{"test"},
		}, "just_for_test")
		So(err, ShouldBeNil)
		persistence := p.(*etcdPersistence)
		persistence.client = kvClient
		Convey("test read credential type cloud", func() {
			subID := vanus.NewID()
			a, _ := crypto.AesEncrypt("test_access_key_id", persistence.cipherKey)
			s, _ := crypto.AesEncrypt("test_secret_access_key", persistence.cipherKey)
			v, _ := json.Marshal(primitive.SinkCredential{
				Type:            primitive.CloudCredentialType,
				AccessKeyID:     a,
				SecretAccessKey: s,
			})
			kvClient.EXPECT().Get(ctx, persistence.getKey(subID)).Return(v, nil)
			credential, err := persistence.Read(ctx, subID)
			So(err, ShouldBeNil)
			So(credential.AccessKeyID, ShouldEqual, "test_access_key_id")
			So(credential.SecretAccessKey, ShouldEqual, "test_secret_access_key")
		})
		Convey("test write", func() {
			subID := vanus.NewID()
			credential := primitive.SinkCredential{
				Type:            primitive.CloudCredentialType,
				AccessKeyID:     "test_access_key_id",
				SecretAccessKey: "test_secret_access_key",
			}
			Convey("test create", func() {
				kvClient.EXPECT().Exists(ctx, persistence.getKey(subID)).Return(false, nil)
				kvClient.EXPECT().Create(ctx, persistence.getKey(subID), gomock.Any()).Return(nil)
				err := persistence.Write(ctx, subID, credential)
				So(err, ShouldBeNil)
			})
			Convey("test update", func() {
				kvClient.EXPECT().Exists(ctx, persistence.getKey(subID)).Return(true, nil)
				kvClient.EXPECT().Update(ctx, persistence.getKey(subID), gomock.Any()).Return(nil)
				err := persistence.Write(ctx, subID, credential)
				So(err, ShouldBeNil)
			})
		})
	})
}
