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

package block_test

import (
	// standard libraries.
	"math/rand"
	"testing"
	"time"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	blktest "github.com/linkall-labs/vanus/internal/store/block/testing"
)

var alphabet = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func TestEmptyEntry(t *testing.T) {
	rand.Seed(time.Now().Unix())

	Convey("empty entry", t, func() {
		ent := block.EmptyEntry{}

		for i := 0; i < 10; i++ {
			ord := rand.Int()
			So(ent.Get(ord), ShouldBeNil)
			So(ent.GetBytes(ord), ShouldBeNil)
			So(ent.GetString(ord), ShouldBeEmpty)
			So(ent.GetUint16(ord), ShouldBeZeroValue)
			So(ent.GetUint64(ord), ShouldBeZeroValue)
			So(ent.GetInt64(ord), ShouldBeZeroValue)
			So(ent.GetTime(ord), ShouldBeZeroValue)
		}

		// So(ent.GetExtensionAttribute(nil), ShouldBeNil)

		var count int
		ent.RangeExtensionAttributes(func(attr, val []byte) {
			count++
		})
		So(count, ShouldEqual, 0)
	})

	Convey("empty entry ext", t, func() {
		ext := block.EmptyEntryExt{}

		So(ext.OptionalAttributeCount(), ShouldEqual, 0)

		for i := 0; i < 10; i++ {
			ord := rand.Int()
			So(ext.Get(ord), ShouldBeNil)
			So(ext.GetBytes(ord), ShouldBeNil)
			So(ext.GetString(ord), ShouldBeEmpty)
			So(ext.GetUint16(ord), ShouldBeZeroValue)
			So(ext.GetUint64(ord), ShouldBeZeroValue)
			So(ext.GetInt64(ord), ShouldBeZeroValue)
			So(ext.GetTime(ord), ShouldBeZeroValue)
		}

		count := 0
		ext.RangeOptionalAttributes(func(ordinal int, val interface{}) {
			count++
		})
		So(count, ShouldEqual, 0)

		So(ext.ExtensionAttributeCount(), ShouldEqual, 0)

		// So(ent.GetExtensionAttribute(nil), ShouldBeNil)

		count = 0
		ext.RangeExtensionAttributes(func(attr, val []byte) {
			count++
		})
		So(count, ShouldEqual, 0)
	})

	Convey("entry ext wrapper", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		ext := blktest.NewMockEntryExt(ctrl)
		w := block.EntryExtWrapper{E: ext}

		n := rand.Int()
		ext.EXPECT().OptionalAttributeCount().Times(1).Return(n)
		So(w.OptionalAttributeCount(), ShouldEqual, n)

		for i := 0; i < 10; i++ {
			ord := rand.Int()

			n = rand.Int()
			ext.EXPECT().Get(ord).Times(1).DoAndReturn(func(ordinal int) interface{} {
				So(ordinal, ShouldEqual, ord)
				return n
			})
			So(w.Get(ord), ShouldEqual, n)

			s := randString(rand.Intn(10))
			buf := []byte(s)
			ext.EXPECT().GetBytes(ord).Times(1).DoAndReturn(func(ordinal int) []byte {
				So(ordinal, ShouldEqual, ord)
				return buf
			})
			So(w.GetBytes(ord), ShouldResemble, buf)

			ext.EXPECT().GetString(ord).Times(1).DoAndReturn(func(ordinal int) string {
				So(ordinal, ShouldEqual, ord)
				return s
			})
			So(w.GetString(ord), ShouldResemble, s)

			u16 := uint16(rand.Uint32())
			ext.EXPECT().GetUint16(ord).Times(1).DoAndReturn(func(ordinal int) uint16 {
				So(ordinal, ShouldEqual, ord)
				return u16
			})
			So(w.GetUint16(ord), ShouldEqual, u16)

			u64 := rand.Uint64()
			ext.EXPECT().GetUint64(ord).Times(1).DoAndReturn(func(ordinal int) uint64 {
				So(ordinal, ShouldEqual, ord)
				return u64
			})
			So(w.GetUint64(ord), ShouldEqual, u64)

			n64 := rand.Int63()
			ext.EXPECT().GetInt64(ord).Times(1).DoAndReturn(func(ordinal int) int64 {
				So(ordinal, ShouldEqual, ord)
				return n64
			})
			So(w.GetInt64(ord), ShouldEqual, n64)

			t := time.Now()
			ext.EXPECT().GetTime(ord).Times(1).DoAndReturn(func(ordinal int) time.Time {
				So(ordinal, ShouldEqual, ord)
				return t
			})
			So(w.GetTime(ord), ShouldEqual, t)
		}

		fn0 := func(int, interface{}) {}
		ext.EXPECT().RangeOptionalAttributes(Any()).Times(1).DoAndReturn(func(f func(int, interface{})) {
			So(f, ShouldEqual, fn0)
		})
		w.RangeOptionalAttributes(fn0)

		n = rand.Int()
		ext.EXPECT().ExtensionAttributeCount().Times(1).Return(n)
		So(w.ExtensionAttributeCount(), ShouldEqual, n)

		attr := []byte(randString(rand.Intn(10)))
		ext.EXPECT().GetExtensionAttribute(attr).Times(1).DoAndReturn(func(attr []byte) []byte {
			So(attr, ShouldResemble, attr)
			return attr
		})
		So(w.GetExtensionAttribute(attr), ShouldResemble, attr)

		fn1 := func([]byte, []byte) {}
		ext.EXPECT().RangeExtensionAttributes(Any()).Times(1).DoAndReturn(func(f func([]byte, []byte)) {
			So(f, ShouldEqual, fn1)
		})
		w.RangeExtensionAttributes(fn1)
	})
}
