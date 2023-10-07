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

package snowflake

import (
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	vanus "github.com/vanus-labs/vanus/api/vsr"
)

func TestNewID(t *testing.T) {
	Convey("test new id not equal", t, func() {
		var id1, id2 vanus.ID
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			id1 = NewTestID() // TODO
		}()
		go func() {
			defer wg.Done()
			id2 = NewTestID() // TODO
		}()
		wg.Wait()
		So(id1, ShouldNotEqual, id2)
	})
}
