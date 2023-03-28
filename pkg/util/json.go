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

package util

import (
	"fmt"

	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"

	"github.com/vanus-labs/vanus/pkg/errors"
)

func ParseJSON(b []byte) (interface{}, error) {
	return oj.Parse(b)
}

func GetJSONValue(data interface{}, path string) (interface{}, error) {
	p, err := jp.ParseString(path)
	if err != nil {
		return nil, errors.ErrParseJSONPath.WithMessage(fmt.Sprintf("json path %s invalid", path)).Wrap(err)
	}
	res := p.Get(data)
	switch len(res) {
	case 0:
		return nil, errors.ErrJSONPathNoExist
	case 1:
		return res[0], nil
	default:
		return res, nil
	}
}
