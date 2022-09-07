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

package vsb

import (
	// standard libraries.
	"fmt"
	"os"
	"path/filepath"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

func TestEngine_ResolvePath(t *testing.T) {
	dir := os.TempDir()
	id := vanus.NewID()

	Convey("resolve path", t, func() {
		e := engine{
			dir: dir,
		}
		path := e.resolvePath(id)
		So(path, ShouldEqual, filepath.Join(dir,
			fmt.Sprintf("%020d.vsb", id.Uint64())))
	})
}
