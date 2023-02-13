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
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	blktest "github.com/linkall-labs/vanus/internal/store/block/testing"
)

var fragmentData = append([]byte{
	0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
}, []byte("Payload")...)

func TestFragment(t *testing.T) {
	Convey("fragment", t, func() {
		frag := block.NewFragment(fragmentData)
		So(frag.Payload(), ShouldResemble, fragmentData[8:])
		So(frag.Size(), ShouldEqual, len(fragmentData)-8)
		So(frag.StartOffset(), ShouldEqual, int64(4096))
		So(frag.EndOffset(), ShouldEqual, int64(4096-8+len(fragmentData)))

		marshaler, ok := frag.(block.FragmentMarshaler)
		So(ok, ShouldBeTrue)
		data, err := marshaler.MarshalFragment()
		So(err, ShouldBeNil)
		So(data, ShouldResemble, fragmentData)
	})
}

func TestMarshalFragment(t *testing.T) {
	Convey("marshal fragment", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		Convey("directly", func() {
			frag := blktest.NewMockFragment(ctrl)
			frag.EXPECT().Size().AnyTimes().Return(len(fragmentData) - 8)
			frag.EXPECT().StartOffset().AnyTimes().Return(int64(4096))
			frag.EXPECT().Payload().AnyTimes().Return(fragmentData[8:])

			data, err := block.MarshalFragment(frag)
			So(err, ShouldBeNil)
			So(data, ShouldResemble, fragmentData)
		})

		Convey("by marshaler", func() {
			frag := struct {
				*blktest.MockFragment
				*blktest.MockFragmentMarshaler
			}{
				MockFragment:          blktest.NewMockFragment(ctrl),
				MockFragmentMarshaler: blktest.NewMockFragmentMarshaler(ctrl),
			}
			frag.MockFragmentMarshaler.EXPECT().MarshalFragment().Return(fragmentData, nil)

			data, err := block.MarshalFragment(frag)
			So(err, ShouldBeNil)
			So(data, ShouldResemble, fragmentData)
		})
	})
}
