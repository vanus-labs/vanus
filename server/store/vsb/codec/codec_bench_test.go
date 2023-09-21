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

package codec

import (
	// standard libraries.
	"bytes"
	"testing"

	// this project.
	ceschema "github.com/vanus-labs/vanus/server/store/schema/ce"
	ceconvert "github.com/vanus-labs/vanus/server/store/schema/ce/convert"
	cetest "github.com/vanus-labs/vanus/server/store/schema/ce/testing"
	vsbentry "github.com/vanus-labs/vanus/server/store/vsb/entry"
	vsbtest "github.com/vanus-labs/vanus/server/store/vsb/testing"
)

func BenchmarkEntryEncoder_CEEntry(b *testing.B) {
	event := cetest.MakeEvent1()
	ceEntry := ceconvert.ToEntry(event)
	entry := vsbentry.Wrap(ceEntry, ceschema.CloudEvent, 1, cetest.Stime)

	enc := NewEncoder()
	buf := make([]byte, enc.Size(entry))

	b.Run("EntryEncoder: marshal ce entry", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = enc.MarshalTo(entry, buf)
		}
	})

	if !bytes.Equal(buf, vsbtest.EntryData1) {
		panic("not equal")
	}
}
