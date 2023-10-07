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

package json

import (
	// this project.
	"github.com/vanus-labs/vanus/lib/bytes"
	"github.com/vanus-labs/vanus/pkg/template"
)

func Compile(text string) (template.Template, error) {
	var parser templateParser
	root, err := parser.parse(text)
	if err != nil {
		return nil, err
	}

	var generator templateGenerator
	segments := generator.generate(root)

	// TODO(james.yin): check segments

	return &jsonTemplate{segments: segments}, nil
}

type jsonTemplate struct {
	segments []templateSegment
}

// Make sure jsonTemplate implements template.Template.
var _ template.Template = (*jsonTemplate)(nil)

func (t *jsonTemplate) ContentType() string {
	return "application/json"
}

func (t *jsonTemplate) Execute(model interface{}, variables map[string]interface{}) ([]byte, error) {
	var buf executeBuffer
	for _, segment := range t.segments {
		if err := segment.RenderTo(&buf, model, variables); err != nil {
			return nil, err
		}
	}
	return buf.buf, nil
}

type executeBuffer struct {
	buf []byte
}

// Make sure executeBuffer implements bytes.LastByteWriter.
var _ bytes.LastByteWriter = (*executeBuffer)(nil)

func (b *executeBuffer) Write(p []byte) (n int, err error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *executeBuffer) LastByte() (byte, bool) {
	n := len(b.buf)
	if n == 0 {
		return 0, false
	}
	return b.buf[n-1], true
}

func (b *executeBuffer) TruncateLastByte() {
	n := len(b.buf)
	if n != 0 {
		b.buf = b.buf[:n-1]
	}
}
