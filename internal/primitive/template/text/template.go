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

package text

import (
	// standard libraries.
	"bytes"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/template"
)

func Compile(text string) (template.Template, error) {
	segments, err := parse(text)
	if err != nil {
		return nil, err
	}

	// TODO(james.yin): check segments

	return &textTemplate{segments: segments}, nil
}

type textTemplate struct {
	segments []templateSegment
}

// Make sure textTemplate implements template.Template.
var _ template.Template = (*textTemplate)(nil)

func (t *textTemplate) ContentType() string {
	return "text/plain"
}

func (t *textTemplate) Execute(model interface{}, variables map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	for _, segment := range t.segments {
		if err := segment.RenderTo(&buf, model, variables); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
