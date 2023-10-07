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
	// third-party libraries.
	"github.com/ohler55/ojg/jp"

	// this project.
	"github.com/vanus-labs/vanus/lib/bytes"
)

type templateSegment interface {
	RenderTo(w bytes.LastByteWriter, model any, variables map[string]any) error
}

type literalSegment struct {
	val []byte
}

// Make sure literalSegment implements templateSegment.
var _ templateSegment = (*literalSegment)(nil)

func (s *literalSegment) RenderTo(w bytes.LastByteWriter, _ any, _ map[string]any) error {
	b, ok := w.LastByte()
	if !ok {
		return ignoreCount(w.Write(s.val))
	}
	if b == ',' && (s.val[0] == '}' || s.val[0] == ']') {
		w.TruncateLastByte()
	}
	// skip leading comma
	if s.val[0] == ',' && (b == ',' || b == '{' || b == '[') {
		if len(s.val) <= 1 {
			return nil
		}
		return ignoreCount(w.Write(s.val[1:]))
	}
	return ignoreCount(w.Write(s.val))
}

type variableSegment struct {
	name string
}

// Make sure variableSegment implements templateSegment.
var _ templateSegment = (*variableSegment)(nil)

func (s *variableSegment) RenderTo(w bytes.LastByteWriter, _ any, variables map[string]any) error {
	// Variables MUST be defined. But to prevent corner cases, write a "null".
	v := variables[s.name]
	return writeJSON(w, v)
}

type variableStringSegment struct {
	name string
}

// Make sure variableStringSegment implements templateSegment.
var _ templateSegment = (*variableStringSegment)(nil)

func (s *variableStringSegment) RenderTo(w bytes.LastByteWriter, _ any, variables map[string]any) error {
	// Variables MUST be defined. But to prevent corner cases, keep it empty.
	v, ok := variables[s.name]
	if !ok {
		return nil
	}
	return writeInJSONString(w, v)
}

type memberSegment struct {
	key   []byte
	value jp.Expr
}

// Make sure memberSegment implements templateSegment.
var _ templateSegment = (*memberSegment)(nil)

func (s *memberSegment) RenderTo(w bytes.LastByteWriter, model any, _ map[string]any) error {
	results := s.value.Get(model)
	if len(results) == 0 {
		return nil
	}

	b, _ := w.LastByte()
	if b != ',' && b != '{' {
		if _, err := w.Write([]byte{','}); err != nil {
			return err
		}
	}

	if _, err := w.Write(s.key); err != nil {
		return err
	}

	if len(results) == 1 {
		return writeJSON(w, results[0])
	}
	return writeJSON(w, results)
}

type elementSegment struct {
	value jp.Expr
}

// Make sure elementSegment implements templateSegment.
var _ templateSegment = (*elementSegment)(nil)

func (s *elementSegment) RenderTo(w bytes.LastByteWriter, model any, _ map[string]any) error {
	results := s.value.Get(model)
	if len(results) == 0 {
		return nil
	}

	b, _ := w.LastByte()
	if b != ',' && b != '[' {
		if _, err := w.Write([]byte{','}); err != nil {
			return err
		}
	}

	if len(results) == 1 {
		return writeJSON(w, results[0])
	}
	return writeJSON(w, results)
}

type jsonPathStringSegment struct {
	path jp.Expr
}

// Make sure jsonPathStringSegment implements templateSegment.
var _ templateSegment = (*jsonPathStringSegment)(nil)

func (s *jsonPathStringSegment) RenderTo(w bytes.LastByteWriter, model any, _ map[string]any) error {
	results := s.path.Get(model)
	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return writeInJSONString(w, results[0])
	}
	return writeJSONInJSONString(w, results)
}
