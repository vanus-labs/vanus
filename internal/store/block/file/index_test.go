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

package file

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIndex_Marshal(t *testing.T) {
	Convey("test marshall & unmarshall of index", t, func() {
		rd := rand.New(rand.NewSource(time.Now().UnixNano()))
		idx1 := index{
			offset: rd.Int63(),
			length: rd.Int31(),
		}

		data := make([]byte, 12)
		n, err := idx1.MarshalTo(data)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 12)

		nIdx, err := unmarshalIndex(data)
		So(err, ShouldBeNil)
		So(idx1, ShouldResemble, nIdx)
		So(nIdx.StartOffset(), ShouldEqual, nIdx.offset)
		So(nIdx.EndOffset(), ShouldEqual, nIdx.offset+int64(nIdx.length))

		n, err = idx1.MarshalTo(make([]byte, rd.Int31n(12)))
		So(n, ShouldBeZeroValue)
		So(err, ShouldEqual, bytes.ErrTooLarge)

		nIdx, err = unmarshalIndex(make([]byte, rd.Int31n(12)))
		So(err, ShouldBeNil)
		So(n, ShouldBeZeroValue)
		So(nIdx, ShouldResemble, index{})
	})
}
