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
	stdCtx "context"
	"os"
	"path/filepath"
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBlock_Destroy(t *testing.T) {
	Convey("", t, func() {
		dir, err := os.MkdirTemp("", "*")
		defer func() {
			_ = os.RemoveAll(dir)
		}()
		workDir := filepath.Join(dir, "vanus/test/store/test")
		_ = os.MkdirAll(workDir, 0777)
		blk, err := Create(stdCtx.Background(), workDir, vanus.NewID(), 1024*1024*64)
		So(err, ShouldBeNil)
		f, err := os.Open(blk.Path())
		So(err, ShouldBeNil)
		_ = f.Close()

		err = blk.Destroy(stdCtx.Background())
		So(err, ShouldBeNil)
		_, err = os.Open(blk.Path())
		So(os.IsNotExist(err), ShouldBeTrue)
	})
}
