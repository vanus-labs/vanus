// Copyright 2023 Linkall Inc.
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

package manager

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

func makeNamespace(name string) *metadata.Namespace {
	id := vanus.NewTestID()
	return &metadata.Namespace{
		Name:      name,
		ID:        id,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func TestNamespaceManager_Init(t *testing.T) {
	Convey("namespace init", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewNamespaceManager(kvClient).(*namespaceManager)
		md1 := makeNamespace("test1")
		md2 := makeNamespace("test2")
		b1, _ := json.Marshal(md1)
		b2, _ := json.Marshal(md2)
		kvClient.EXPECT().List(gomock.Any(), gomock.Eq(kv.NamespaceAllKey())).Return(
			[]kv.Pair{
				{
					Key:   kv.NamespaceKey(md1.ID),
					Value: b1,
				},
				{
					Key:   kv.NamespaceKey(md2.ID),
					Value: b2,
				},
			}, nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
		So(len(m.namespaces), ShouldEqual, 2)
		_, exist := m.namespaces[md1.ID]
		So(exist, ShouldBeTrue)
		_, exist = m.namespaces[md2.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestMockNamespaceManager_AddNamespace(t *testing.T) {
	Convey("namespace add", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewNamespaceManager(kvClient).(*namespaceManager)
		md := makeNamespace("test1")
		kvClient.EXPECT().Set(gomock.Any(), gomock.Eq(kv.NamespaceKey(md.ID)), gomock.Any()).Return(nil)
		err := m.AddNamespace(ctx, md)
		So(err, ShouldBeNil)
		So(len(m.namespaces), ShouldEqual, 1)
		_, exist := m.namespaces[md.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestMockNamespaceManager_DeleteNamespace(t *testing.T) {
	Convey("namespace delete", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewNamespaceManager(kvClient).(*namespaceManager)
		md := makeNamespace("test1")
		Convey("namespace no exist", func() {
			err := m.DeleteNamespace(ctx, md.ID)
			So(err, ShouldBeNil)
		})
		Convey("namespace default", func() {
			md = makeNamespace(primitive.DefaultNamespace)
			m.namespaces[md.ID] = md
			err := m.DeleteNamespace(ctx, md.ID)
			So(err, ShouldNotBeNil)
		})
		Convey("namespace system", func() {
			md = makeNamespace(primitive.SystemNamespace)
			m.namespaces[md.ID] = md
			err := m.DeleteNamespace(ctx, md.ID)
			So(err, ShouldNotBeNil)
		})
		Convey("namespace exist", func() {
			m.namespaces[md.ID] = md
			kvClient.EXPECT().Delete(gomock.Any(), gomock.Eq(kv.NamespaceKey(md.ID))).Return(nil)
			err := m.DeleteNamespace(ctx, md.ID)
			So(err, ShouldBeNil)
			So(len(m.namespaces), ShouldEqual, 0)
			_, exist := m.namespaces[md.ID]
			So(exist, ShouldBeFalse)
		})
	})
}

func TestMockNamespaceManager_GetNamespace(t *testing.T) {
	Convey("namespace get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewNamespaceManager(kvClient).(*namespaceManager)
		md := makeNamespace("test1")
		Convey("namespace no exist", func() {
			get := m.GetNamespace(ctx, md.ID)
			So(get, ShouldBeNil)
		})
		Convey("namespace exist", func() {
			m.namespaces[md.ID] = md
			get := m.GetNamespace(ctx, md.ID)
			So(get, ShouldNotBeNil)
		})
		Convey("namespace get by name", func() {
			m.namespaces[md.ID] = md
			get := m.GetNamespaceByName(ctx, md.Name)
			So(get, ShouldNotBeNil)
		})
	})
}

func TestMockNamespaceManager_ListNamespace(t *testing.T) {
	Convey("namespace get", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		kvClient := kv.NewMockClient(ctrl)
		m := NewNamespaceManager(kvClient).(*namespaceManager)
		md1 := makeNamespace("test1")
		md2 := makeNamespace("test2")
		list := m.ListNamespace(ctx)
		So(len(list), ShouldEqual, 0)
		m.namespaces[md1.ID] = md1
		m.namespaces[md2.ID] = md2
		list = m.ListNamespace(ctx)
		So(len(list), ShouldEqual, 2)
	})
}
