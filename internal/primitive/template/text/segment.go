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
	"io"

	// third-party libraries.
	"github.com/ohler55/ojg/jp"
)

type templateSegment interface {
	RenderTo(w io.Writer, model any, variables map[string]any) error
}

type textSegment struct {
	text []byte
}

// Make sure literalSegment implements templateSegment.
var _ templateSegment = (*textSegment)(nil)

func (s *textSegment) RenderTo(w io.Writer, _ any, _ map[string]any) error {
	return ignoreCount(w.Write(s.text))
}

type variableSegment struct {
	name string
}

// Make sure variableSegment implements templateSegment.
var _ templateSegment = (*variableSegment)(nil)

func (s *variableSegment) RenderTo(w io.Writer, _ any, variables map[string]any) error {
	// Variables MUST be defined. But to prevent corner cases, keep it empty.
	v, ok := variables[s.name]
	if !ok {
		return nil
	}
	return write(w, v)
}

type jsonPathSegment struct {
	path jp.Expr
}

// Make sure jsonPathSegment implements templateSegment.
var _ templateSegment = (*jsonPathSegment)(nil)

func (s *jsonPathSegment) RenderTo(w io.Writer, model any, _ map[string]any) error {
	results := s.path.Get(model)
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return write(w, results[0])
	}
	return write(w, results)
}
