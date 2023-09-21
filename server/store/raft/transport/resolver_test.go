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

package transport

import (
	// standard libraries.

	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestResolver(t *testing.T) {
	Convey("test resolver", t, func() {
		simpleResolver := NewSimpleResolver()
		nodeID := 1
		endpoint := "127.0.0.1:2000"

		Convey("test Register and Resolve method", func() {
			So(simpleResolver.Resolve(uint64(nodeID)), ShouldBeEmpty)
			simpleResolver.Register(uint64(nodeID), endpoint)
			So(simpleResolver.Resolve(uint64(nodeID)), ShouldEqual, endpoint)
			simpleResolver.Register(uint64(nodeID+1), endpoint)
			So(simpleResolver.Resolve(uint64(nodeID+1)), ShouldEqual, endpoint)
		})

		Convey("test Unregister method", func() {
			simpleResolver.Register(uint64(nodeID), endpoint)
			So(simpleResolver.Resolve(uint64(nodeID)), ShouldEqual, endpoint)
			simpleResolver.Unregister(uint64(nodeID))
			So(simpleResolver.Resolve(uint64(nodeID)), ShouldBeEmpty)
		})
	})
}
